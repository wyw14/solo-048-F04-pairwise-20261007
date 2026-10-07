package pump

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"

	"heatpulse/internal/audit"
	"heatpulse/internal/hydraulic"
	"heatpulse/internal/network"
)

type Command struct {
	ID        string    `json:"id"`
	ZoneID    string    `json:"zone_id"`
	Stage     int       `json:"stage"`
	Epoch     uint64    `json:"epoch"`
	Key       string    `json:"key,omitempty"`
	Status    string    `json:"status"`
	IssuedAt  time.Time `json:"issued_at"`
	AppliedAt time.Time `json:"applied_at,omitempty"`
}

type Service struct {
	interlock *hydraulic.Interlock
	audit     *audit.Recorder
	applyMu   sync.Mutex
	mu        sync.RWMutex
	commands  map[string]Command
	byKey     map[string]string
	epochs    map[string]uint64
}

func NewService(interlock *hydraulic.Interlock, recorder *audit.Recorder) *Service {
	return &Service{
		interlock: interlock,
		audit:     recorder,
		commands:  make(map[string]Command),
		byKey:     make(map[string]string),
		epochs:    make(map[string]uint64),
	}
}

func (s *Service) SetStage(ctx context.Context, zoneID string, stage int, epoch uint64, key, actor string) (network.Zone, Command, error) {
	if err := ctx.Err(); err != nil {
		return network.Zone{}, Command{}, err
	}
	command, duplicate := s.claim(zoneID, stage, epoch, key)
	if duplicate {
		zone, err := s.interlock.Zone(zoneID)
		if err != nil {
			return network.Zone{}, command, err
		}
		return zone, command, nil
	}
	s.applyMu.Lock()
	defer s.applyMu.Unlock()
	if fence := s.fence(zoneID); epoch < fence {
		command.Status = "rejected_old_epoch"
		s.save(command)
		err := fmt.Errorf("controller epoch %d retired for zone %s (current %d)", epoch, zoneID, fence)
		_ = s.audit.Rejected("pump.stage", zoneID, actor, err.Error(), map[string]any{"command_id": command.ID, "stage": stage, "epoch": epoch})
		return network.Zone{}, command, err
	}
	zone, err := s.interlock.StagePump(zoneID, stage)
	if err != nil {
		command.Status = "rejected"
		s.save(command)
		_ = s.audit.Rejected("pump.stage", zoneID, actor, err.Error(), map[string]any{"command_id": command.ID, "stage": stage, "epoch": epoch})
		return network.Zone{}, command, err
	}
	command.Status = "applied"
	command.AppliedAt = time.Now().UTC()
	s.save(command)
	s.raiseFence(zoneID, epoch)
	if err := s.audit.Accepted("pump.stage", zoneID, actor, "pump stage applied", map[string]any{"command_id": command.ID, "stage": stage, "epoch": epoch}); err != nil {
		return network.Zone{}, command, fmt.Errorf("record pump command: %w", err)
	}
	return zone, command, nil
}

// claim records a fresh pending command, or returns the command already stored
// for the caller's idempotency key so retries replay the original outcome.
func (s *Service) claim(zoneID string, stage int, epoch uint64, key string) (command Command, duplicate bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if key != "" {
		if id, ok := s.byKey[dedupeKey(zoneID, key)]; ok {
			return s.commands[id], true
		}
	}
	command = Command{ID: uuid.NewString(), ZoneID: zoneID, Stage: stage, Epoch: epoch, Key: key, Status: "pending", IssuedAt: time.Now().UTC()}
	s.commands[command.ID] = command
	if key != "" {
		s.byKey[dedupeKey(zoneID, key)] = command.ID
	}
	return command, false
}

func dedupeKey(zoneID, key string) string {
	return zoneID + "\x00" + key
}

func (s *Service) fence(zoneID string) uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.epochs[zoneID]
}

func (s *Service) raiseFence(zoneID string, epoch uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if epoch > s.epochs[zoneID] {
		s.epochs[zoneID] = epoch
	}
}

func (s *Service) save(command Command) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.commands[command.ID] = command
}

func (s *Service) Command(commandID string) (Command, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	command, ok := s.commands[commandID]
	return command, ok
}

func (s *Service) CommandsForZone(zoneID string) []Command {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]Command, 0)
	for _, command := range s.commands {
		if command.ZoneID == zoneID {
			result = append(result, command)
		}
	}
	return result
}
