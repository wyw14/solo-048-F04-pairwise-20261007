package pump

import (
	"context"
	"path/filepath"
	"testing"

	"heatpulse/internal/audit"
	"heatpulse/internal/hydraulic"
	"heatpulse/internal/network"
	"heatpulse/internal/store"
)

func newTestService(t *testing.T, stage int) (*Service, *network.State) {
	t.Helper()
	state := network.NewState([]network.Zone{
		{ID: "north", Name: "北区", Phase: network.PhaseActive, PumpStage: stage, BypassOpen: true},
	})
	interlock := hydraulic.NewInterlock(state)
	recorder := audit.NewRecorder(store.NewEventLog(filepath.Join(t.TempDir(), "events.log")))
	return NewService(interlock, recorder), state
}

func TestSetStageReplaysIdempotencyKey(t *testing.T) {
	service, state := newTestService(t, 1)
	ctx := context.Background()

	zone, first, err := service.SetStage(ctx, "north", 2, 7, "req-1", "dispatcher")
	if err != nil {
		t.Fatalf("first request: %v", err)
	}
	if first.Status != "applied" || zone.PumpStage != 2 {
		t.Fatalf("first request = %+v, zone %+v", first, zone)
	}

	zone, replay, err := service.SetStage(ctx, "north", 2, 7, "req-1", "dispatcher")
	if err != nil {
		t.Fatalf("replay request: %v", err)
	}
	if replay.ID != first.ID {
		t.Fatalf("replay created new command %s, want original %s", replay.ID, first.ID)
	}
	if replay.Status != "applied" || replay.AppliedAt != first.AppliedAt {
		t.Fatalf("replay = %+v, want original outcome %+v", replay, first)
	}
	if zone.PumpStage != 2 {
		t.Fatalf("zone stage = %d, want 2", zone.PumpStage)
	}
	if got := len(service.CommandsForZone("north")); got != 1 {
		t.Fatalf("commands for zone = %d, want 1", got)
	}

	// A different key is a new command, even with identical payload.
	_, other, err := service.SetStage(ctx, "north", 2, 7, "req-2", "dispatcher")
	if err != nil {
		t.Fatalf("distinct key request: %v", err)
	}
	if other.ID == first.ID {
		t.Fatalf("distinct key reused command %s", other.ID)
	}
	if got := len(service.CommandsForZone("north")); got != 2 {
		t.Fatalf("commands for zone = %d, want 2", got)
	}
	if current, _ := state.Get("north"); current.PumpStage != 2 {
		t.Fatalf("zone stage = %d, want 2", current.PumpStage)
	}
}

func TestSetStageRetiresOldEpochToHistory(t *testing.T) {
	service, state := newTestService(t, 1)
	ctx := context.Background()

	if _, _, err := service.SetStage(ctx, "north", 3, 5, "", "dispatcher"); err != nil {
		t.Fatalf("epoch 5 request: %v", err)
	}

	_, stale, err := service.SetStage(ctx, "north", 1, 4, "", "dispatcher")
	if err == nil {
		t.Fatalf("old epoch request should fail")
	}
	if stale.Status != "rejected_old_epoch" {
		t.Fatalf("stale command status = %q, want rejected_old_epoch", stale.Status)
	}
	if current, _ := state.Get("north"); current.PumpStage != 3 {
		t.Fatalf("old epoch changed pump stage to %d, want 3", current.PumpStage)
	}

	// The retired receipt stays queryable history.
	persisted, ok := service.Command(stale.ID)
	if !ok {
		t.Fatalf("stale command %s missing from history", stale.ID)
	}
	if persisted.Status != "rejected_old_epoch" || persisted.Epoch != 4 {
		t.Fatalf("persisted stale command = %+v", persisted)
	}
	if got := len(service.CommandsForZone("north")); got != 2 {
		t.Fatalf("commands for zone = %d, want 2", got)
	}

	// The fence epoch itself still applies; a newer epoch advances the fence.
	if _, equal, err := service.SetStage(ctx, "north", 4, 5, "", "dispatcher"); err != nil {
		t.Fatalf("fence epoch request: %v", err)
	} else if equal.Status != "applied" {
		t.Fatalf("fence epoch command = %+v", equal)
	}
	if _, _, err := service.SetStage(ctx, "north", 2, 6, "", "dispatcher"); err != nil {
		t.Fatalf("epoch 6 request: %v", err)
	}
	if _, lagging, err := service.SetStage(ctx, "north", 0, 5, "", "dispatcher"); err == nil {
		t.Fatalf("epoch 5 after 6 should be retired, got %+v", lagging)
	}
	if current, _ := state.Get("north"); current.PumpStage != 2 {
		t.Fatalf("zone stage = %d, want 2", current.PumpStage)
	}
}

func TestSetStageReplayDoesNotBypassEpochFence(t *testing.T) {
	service, _ := newTestService(t, 0)
	ctx := context.Background()

	_, first, err := service.SetStage(ctx, "north", 1, 2, "req-9", "dispatcher")
	if err != nil {
		t.Fatalf("epoch 2 request: %v", err)
	}
	if _, _, err := service.SetStage(ctx, "north", 3, 4, "", "dispatcher"); err != nil {
		t.Fatalf("epoch 4 request: %v", err)
	}

	// Replaying the old key returns the original command without re-applying it.
	zone, replay, err := service.SetStage(ctx, "north", 1, 2, "req-9", "dispatcher")
	if err != nil {
		t.Fatalf("replay request: %v", err)
	}
	if replay.ID != first.ID || replay.Status != "applied" {
		t.Fatalf("replay = %+v, want original %+v", replay, first)
	}
	if zone.PumpStage != 3 {
		t.Fatalf("replay moved pump stage to %d, want 3", zone.PumpStage)
	}
	if got := len(service.CommandsForZone("north")); got != 2 {
		t.Fatalf("commands for zone = %d, want 2", got)
	}
}
