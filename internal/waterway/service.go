// Package waterway arbitrates access to the pilotage waterway shared by both
// lock chambers. Fleets acquire the waterway by direction when entering or
// leaving a chamber; opposing headings are never granted at the same time.
package waterway

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/journal"
	"github.com/wyw14/cry-96/internal/model"
)

// ConflictError reports that opposing traffic currently holds the waterway.
type ConflictError struct {
	Direction model.Direction
	Phase     model.WaterwayPhase
	Blockers  []model.WaterwayOccupation
}

func (e *ConflictError) Error() string {
	parts := make([]string, 0, len(e.Blockers))
	for _, blocker := range e.Blockers {
		parts = append(parts, fmt.Sprintf("fleet [%s] %s %s at %s since %s",
			strings.Join(blocker.Fleet, ", "), blocker.Phase, blocker.ChamberID,
			blocker.Direction, blocker.AcquiredAt.Format("15:04:05")))
	}
	return fmt.Sprintf("shared waterway blocked for %s %s: %s",
		e.Direction, e.Phase, strings.Join(parts, "; "))
}

// DirectionStatus describes whether traffic of one heading may acquire the waterway.
type DirectionStatus struct {
	Direction model.Direction            `json:"direction"`
	Blocked   bool                       `json:"blocked"`
	Blockers  []model.WaterwayOccupation `json:"blockers,omitempty"`
}

// Status is the operator-facing snapshot of the shared waterway.
type Status struct {
	Occupations []model.WaterwayOccupation `json:"occupations"`
	Directions  []DirectionStatus          `json:"directions"`
	CheckedAt   time.Time                  `json:"checked_at"`
}

// Service grants and releases waterway occupations, journaling every change
// before it becomes visible so in-transit occupations survive restarts.
type Service struct {
	mu       sync.RWMutex
	active   map[uuid.UUID]model.WaterwayOccupation
	recorder *journal.Recorder
	now      func() time.Time
}

func NewService(recorder *journal.Recorder, now func() time.Time) *Service {
	if now == nil {
		now = time.Now
	}
	return &Service{active: make(map[uuid.UUID]model.WaterwayOccupation), recorder: recorder, now: now}
}

// Acquire grants the cycle a waterway occupation for one phase. It is
// idempotent per cycle generation and phase, and refuses while any active
// occupation holds the opposite direction.
func (s *Service) Acquire(ctx context.Context, cycle model.TransitCycle, phase model.WaterwayPhase, fleet []string) (model.WaterwayOccupation, error) {
	if cycle.ID == uuid.Nil || cycle.ChamberID == "" {
		return model.WaterwayOccupation{}, errors.New("waterway occupation requires an active transit cycle")
	}
	if cycle.Stage.Terminal() {
		return model.WaterwayOccupation{}, fmt.Errorf("terminal transit %s cannot occupy the waterway", cycle.Stage)
	}
	if !phase.Valid() {
		return model.WaterwayOccupation{}, fmt.Errorf("invalid waterway phase %q", phase)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, occupation := range s.active {
		if occupation.CycleID == cycle.ID && occupation.Generation == cycle.Generation && occupation.Phase == phase {
			return occupation.Clone(), nil
		}
	}
	var blockers []model.WaterwayOccupation
	for _, occupation := range s.active {
		if occupation.ConflictsWith(cycle.Direction) {
			blockers = append(blockers, occupation.Clone())
		}
	}
	if len(blockers) > 0 {
		sort.Slice(blockers, func(i, j int) bool { return blockers[i].AcquiredAt.Before(blockers[j].AcquiredAt) })
		return model.WaterwayOccupation{}, &ConflictError{Direction: cycle.Direction, Phase: phase, Blockers: blockers}
	}
	occupation, err := model.NewWaterwayOccupation(cycle.ChamberID, cycle.ID, cycle.Generation, cycle.Direction, phase, fleet, s.now())
	if err != nil {
		return model.WaterwayOccupation{}, err
	}
	if _, err := s.recorder.Record(ctx, cycle.ChamberID, model.EventWaterwayAcquired, cycle.ID, cycle.Generation, occupation); err != nil {
		return model.WaterwayOccupation{}, err
	}
	s.active[occupation.ID] = occupation
	return occupation.Clone(), nil
}

// ReleasePhase releases the occupation the cycle holds for one phase.
// Releasing an occupation that is not held is a no-op so retries stay safe.
func (s *Service) ReleasePhase(ctx context.Context, cycle model.TransitCycle, phase model.WaterwayPhase, reason string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, occupation := range s.active {
		if occupation.CycleID == cycle.ID && occupation.Generation == cycle.Generation && occupation.Phase == phase {
			return s.releaseLocked(ctx, occupation, reason)
		}
	}
	return nil
}

// ReleaseAll releases every occupation the cycle still holds. Used when a
// transit is cancelled, emergency-stopped or confirmed completed.
func (s *Service) ReleaseAll(ctx context.Context, cycle model.TransitCycle, reason string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, occupation := range s.active {
		if occupation.CycleID == cycle.ID && occupation.Generation == cycle.Generation {
			if err := s.releaseLocked(ctx, occupation, reason); err != nil {
				return err
			}
		}
	}
	return nil
}

// ReleaseOccupation lets an operator clear one occupation explicitly.
func (s *Service) ReleaseOccupation(ctx context.Context, occupationID uuid.UUID, reason string) (model.WaterwayOccupation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	occupation, ok := s.active[occupationID]
	if !ok {
		return model.WaterwayOccupation{}, fmt.Errorf("waterway occupation %s is not active", occupationID)
	}
	if err := s.releaseLocked(ctx, occupation, reason); err != nil {
		return model.WaterwayOccupation{}, err
	}
	occupation.Active = false
	return occupation, nil
}

// releaseLocked journals the release before dropping the in-memory hold, so a
// crash between the two steps keeps the occupation rather than losing it.
func (s *Service) releaseLocked(ctx context.Context, occupation model.WaterwayOccupation, reason string) error {
	occupation.Release(reason, s.now())
	if _, err := s.recorder.Record(ctx, occupation.ChamberID, model.EventWaterwayReleased, occupation.CycleID, occupation.Generation, occupation); err != nil {
		return err
	}
	delete(s.active, occupation.ID)
	return nil
}

// Active lists current occupations, oldest first.
func (s *Service) Active() []model.WaterwayOccupation {
	s.mu.RLock()
	result := make([]model.WaterwayOccupation, 0, len(s.active))
	for _, occupation := range s.active {
		result = append(result, occupation.Clone())
	}
	s.mu.RUnlock()
	sort.Slice(result, func(i, j int) bool { return result[i].AcquiredAt.Before(result[j].AcquiredAt) })
	return result
}

// Blockers lists the active occupations that conflict with a request for direction.
func (s *Service) Blockers(direction model.Direction) []model.WaterwayOccupation {
	var result []model.WaterwayOccupation
	for _, occupation := range s.Active() {
		if occupation.ConflictsWith(direction) {
			result = append(result, occupation)
		}
	}
	return result
}

// Status reports active occupations and, per direction, what would block a new request.
func (s *Service) Status() Status {
	active := s.Active()
	directions := make([]DirectionStatus, 0, 2)
	for _, direction := range []model.Direction{model.DirectionUpstream, model.DirectionDownstream} {
		entry := DirectionStatus{Direction: direction}
		for _, occupation := range active {
			if occupation.ConflictsWith(direction) {
				entry.Blockers = append(entry.Blockers, occupation)
			}
		}
		entry.Blocked = len(entry.Blockers) > 0
		directions = append(directions, entry)
	}
	return Status{Occupations: active, Directions: directions, CheckedAt: s.now().UTC()}
}

// Restore re-applies a journaled occupation during startup replay.
func (s *Service) Restore(occupation model.WaterwayOccupation) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if occupation.Active {
		s.active[occupation.ID] = occupation.Clone()
		return
	}
	delete(s.active, occupation.ID)
}
