package hydraulic

import (
	"fmt"
	"sync"

	"heatpulse/internal/network"
)

type Candidate struct {
	PumpStage       int    `json:"pump_stage"`
	BypassOpen      bool   `json:"bypass_open"`
	ControllerEpoch uint64 `json:"controller_epoch"`
}

// OldEpochError reports a command fenced out because its controller epoch
// predates the epoch currently recorded on the zone.
type OldEpochError struct {
	Offered uint64
	Current uint64
}

func (e OldEpochError) Error() string {
	return fmt.Sprintf("controller epoch %d retired (current %d)", e.Offered, e.Current)
}

type Interlock struct {
	state *network.State
	mu    sync.Mutex
}

func NewInterlock(state *network.State) *Interlock {
	return &Interlock{state: state}
}

func (i *Interlock) Snapshot(zoneID string) (Candidate, error) {
	zone, ok := i.state.Get(zoneID)
	if !ok {
		return Candidate{}, fmt.Errorf("zone %s not found", zoneID)
	}
	return Candidate{PumpStage: zone.PumpStage, BypassOpen: zone.BypassOpen, ControllerEpoch: zone.ControllerEpoch}, nil
}

func (i *Interlock) Current(zoneID string) (network.Zone, error) {
	zone, ok := i.state.Get(zoneID)
	if !ok {
		return network.Zone{}, fmt.Errorf("zone %s not found", zoneID)
	}
	return zone, nil
}

func Validate(candidate Candidate) error {
	if candidate.PumpStage < 0 || candidate.PumpStage > 4 {
		return fmt.Errorf("pump stage %d outside operating envelope", candidate.PumpStage)
	}
	if candidate.PumpStage >= 3 && !candidate.BypassOpen {
		return fmt.Errorf("hydraulic interlock rejects stage %d with closed bypass", candidate.PumpStage)
	}
	return nil
}

func (i *Interlock) Transition(zoneID string, change func(Candidate) (Candidate, error)) (network.Zone, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.state.Update(zoneID, func(zone *network.Zone) error {
		current := Candidate{PumpStage: zone.PumpStage, BypassOpen: zone.BypassOpen, ControllerEpoch: zone.ControllerEpoch}
		candidate, err := change(current)
		if err != nil {
			return err
		}
		if err := Validate(candidate); err != nil {
			return err
		}
		zone.PumpStage = candidate.PumpStage
		zone.BypassOpen = candidate.BypassOpen
		zone.ControllerEpoch = candidate.ControllerEpoch
		return nil
	})
}

func (i *Interlock) StagePump(zoneID string, stage int, epoch uint64) (network.Zone, error) {
	return i.Transition(zoneID, func(candidate Candidate) (Candidate, error) {
		if epoch < candidate.ControllerEpoch {
			return candidate, OldEpochError{Offered: epoch, Current: candidate.ControllerEpoch}
		}
		candidate.PumpStage = stage
		candidate.ControllerEpoch = epoch
		return candidate, nil
	})
}

func (i *Interlock) SetBypass(zoneID string, open bool) (network.Zone, error) {
	return i.Transition(zoneID, func(candidate Candidate) (Candidate, error) {
		candidate.BypassOpen = open
		return candidate, nil
	})
}
