package pump

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"

	"heatpulse/internal/audit"
	"heatpulse/internal/hydraulic"
	"heatpulse/internal/network"
)

type Command struct {
	ID             string    `json:"id"`
	ZoneID         string    `json:"zone_id"`
	Stage          int       `json:"stage"`
	Epoch          uint64    `json:"epoch"`
	IdempotencyKey string    `json:"idempotency_key,omitempty"`
	Status         string    `json:"status"`
	IssuedAt       time.Time `json:"issued_at"`
	AppliedAt      time.Time `json:"applied_at,omitempty"`
}

type Service struct {
	interlock *hydraulic.Interlock
	audit     *audit.Recorder
	mu        sync.RWMutex
	commands  map[string]Command
	byKey     map[string]string
}

func NewService(interlock *hydraulic.Interlock, recorder *audit.Recorder) *Service {
	return &Service{interlock: interlock, audit: recorder, commands: make(map[string]Command), byKey: make(map[string]string)}
}

func (s *Service) SetStage(ctx context.Context, zoneID string, stage int, epoch uint64, idempotencyKey, actor string) (network.Zone, Command, error) {
	if err := ctx.Err(); err != nil {
		return network.Zone{}, Command{}, err
	}
	s.mu.Lock()
	if idempotencyKey != "" {
		if existingID, ok := s.byKey[idempotencyKey]; ok {
			existing := s.commands[existingID]
			s.mu.Unlock()
			zone, err := s.interlock.Current(existing.ZoneID)
			if err != nil {
				return network.Zone{}, existing, err
			}
			return zone, existing, nil
		}
	}
	command := Command{
		ID: uuid.NewString(), ZoneID: zoneID, Stage: stage, Epoch: epoch,
		IdempotencyKey: idempotencyKey, Status: "pending", IssuedAt: time.Now().UTC(),
	}
	s.commands[command.ID] = command
	if idempotencyKey != "" {
		s.byKey[idempotencyKey] = command.ID
	}
	s.mu.Unlock()

	zone, err := s.interlock.StagePump(zoneID, stage, epoch)
	if err != nil {
		var oldEpoch hydraulic.OldEpochError
		if errors.As(err, &oldEpoch) {
			command.Status = "rejected_old_epoch"
		} else {
			command.Status = "rejected"
		}
		s.save(command)
		_ = s.audit.Rejected("pump.stage", zoneID, actor, err.Error(), map[string]any{"command_id": command.ID, "stage": stage, "epoch": epoch})
		return network.Zone{}, command, err
	}
	command.Status = "applied"
	command.AppliedAt = time.Now().UTC()
	s.save(command)
	if err := s.audit.Accepted("pump.stage", zoneID, actor, "pump stage applied", map[string]any{"command_id": command.ID, "stage": stage, "epoch": epoch}); err != nil {
		return network.Zone{}, command, fmt.Errorf("record pump command: %w", err)
	}
	return zone, command, nil
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
	sort.Slice(result, func(i, j int) bool {
		if result[i].IssuedAt.Equal(result[j].IssuedAt) {
			return result[i].ID < result[j].ID
		}
		return result[i].IssuedAt.Before(result[j].IssuedAt)
	})
	return result
}
