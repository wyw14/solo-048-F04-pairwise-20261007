package pump_test

import (
	"context"
	"path/filepath"
	"testing"

	"heatpulse/internal/audit"
	"heatpulse/internal/hydraulic"
	"heatpulse/internal/network"
	"heatpulse/internal/pump"
	"heatpulse/internal/store"
)

func newService(t *testing.T) (*pump.Service, *network.State) {
	t.Helper()
	state := network.NewState([]network.Zone{{ID: "north", Name: "北环", PumpStage: 2, BypassOpen: true}})
	interlock := hydraulic.NewInterlock(state)
	recorder := audit.NewRecorder(store.NewEventLog(filepath.Join(t.TempDir(), "events.jsonl")))
	return pump.NewService(interlock, recorder), state
}

func TestSetStageDuplicateKeyReturnsOriginalCommand(t *testing.T) {
	service, state := newService(t)
	ctx := context.Background()

	_, first, err := service.SetStage(ctx, "north", 3, 1, "req-1", "dispatcher")
	if err != nil {
		t.Fatalf("first request: %v", err)
	}
	if first.Status != "applied" {
		t.Fatalf("first status = %q, want applied", first.Status)
	}

	// Same idempotency key, different stage: must replay the original command
	// without creating a new one or touching the pump stage.
	zone, second, err := service.SetStage(ctx, "north", 4, 1, "req-1", "dispatcher")
	if err != nil {
		t.Fatalf("duplicate request: %v", err)
	}
	if second.ID != first.ID {
		t.Fatalf("duplicate returned command %s, want original %s", second.ID, first.ID)
	}
	if second.Stage != 3 || second.Status != "applied" {
		t.Fatalf("duplicate command mutated: %+v", second)
	}
	if zone.PumpStage != 3 {
		t.Fatalf("zone stage = %d, want 3 (duplicate must not re-apply)", zone.PumpStage)
	}
	if got := len(service.CommandsForZone("north")); got != 1 {
		t.Fatalf("commands for zone = %d, want 1", got)
	}
	if current, _ := state.Get("north"); current.PumpStage != 3 {
		t.Fatalf("state stage = %d, want 3", current.PumpStage)
	}
}

func TestSetStageOldEpochOnlyEntersHistory(t *testing.T) {
	service, state := newService(t)
	ctx := context.Background()

	if _, _, err := service.SetStage(ctx, "north", 3, 5, "req-epoch-5", "dispatcher"); err != nil {
		t.Fatalf("epoch 5 request: %v", err)
	}

	_, stale, err := service.SetStage(ctx, "north", 4, 2, "req-epoch-2", "dispatcher")
	if err == nil {
		t.Fatal("old epoch request must fail")
	}
	if stale.Status != "rejected_old_epoch" {
		t.Fatalf("stale status = %q, want rejected_old_epoch", stale.Status)
	}

	// The receipt is history only: the zone pump stage and epoch stay put.
	zone, ok := state.Get("north")
	if !ok {
		t.Fatal("zone north missing")
	}
	if zone.PumpStage != 3 {
		t.Fatalf("zone stage = %d, want 3 (old epoch must not change pump level)", zone.PumpStage)
	}
	if zone.ControllerEpoch != 5 {
		t.Fatalf("zone epoch = %d, want 5", zone.ControllerEpoch)
	}

	// ...but it is retained and queryable.
	stored, ok := service.Command(stale.ID)
	if !ok {
		t.Fatal("old epoch command not retained")
	}
	if stored.Status != "rejected_old_epoch" {
		t.Fatalf("stored status = %q, want rejected_old_epoch", stored.Status)
	}
	if got := len(service.CommandsForZone("north")); got != 2 {
		t.Fatalf("commands for zone = %d, want 2 (applied + old epoch receipt)", got)
	}
}

func TestSetStageCommandStatePersistsAcrossQueries(t *testing.T) {
	service, _ := newService(t)
	ctx := context.Background()

	_, applied, err := service.SetStage(ctx, "north", 3, 1, "req-1", "dispatcher")
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if _, _, err := service.SetStage(ctx, "north", 1, 2, "req-2", "dispatcher"); err != nil {
		t.Fatalf("second apply: %v", err)
	}

	// Earlier commands keep their final status when queried later.
	stored, ok := service.Command(applied.ID)
	if !ok {
		t.Fatal("first command lost")
	}
	if stored.Status != "applied" || stored.AppliedAt.IsZero() {
		t.Fatalf("first command not persisted as applied: %+v", stored)
	}

	// Replaying by idempotency key still returns the original applied command.
	_, replay, err := service.SetStage(ctx, "north", 3, 1, "req-1", "dispatcher")
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if replay.ID != applied.ID || replay.Status != "applied" {
		t.Fatalf("replay = %+v, want original applied command", replay)
	}
}

func TestSetStageEqualEpochApplies(t *testing.T) {
	service, state := newService(t)
	ctx := context.Background()

	if _, _, err := service.SetStage(ctx, "north", 3, 4, "req-1", "dispatcher"); err != nil {
		t.Fatalf("epoch 4 request: %v", err)
	}
	zone, command, err := service.SetStage(ctx, "north", 2, 4, "req-2", "dispatcher")
	if err != nil {
		t.Fatalf("equal epoch request: %v", err)
	}
	if command.Status != "applied" || zone.PumpStage != 2 {
		t.Fatalf("equal epoch not applied: status=%q stage=%d", command.Status, zone.PumpStage)
	}
	if current, _ := state.Get("north"); current.ControllerEpoch != 4 {
		t.Fatalf("zone epoch = %d, want 4", current.ControllerEpoch)
	}
}
