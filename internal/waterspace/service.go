package waterspace

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/journal"
	"github.com/wyw14/cry-96/internal/model"
)

var errMissingPlan = errors.New("waterspace: convoy plan not registered for transit")

// Phase identifies which leg of a transit touches a shared reach.
type Phase string

const (
	PhaseEntry Phase = "entry"
	PhaseExit  Phase = "exit"
)

// ReleasedRecord is the durable payload of a waterway.released event.
type ReleasedRecord struct {
	Waterway   string    `json:"waterway"`
	Phase      Phase     `json:"phase"`
	ChamberID  string    `json:"chamber_id"`
	CycleID    uuid.UUID `json:"cycle_id"`
	Generation uint64    `json:"generation"`
	Reason     string    `json:"reason"`
	ReleasedAt time.Time `json:"released_at"`
}

// Verdict tells a caller whether its request currently holds clearance.
type Verdict struct {
	Granted bool   `json:"granted"`
	Reason  string `json:"reason,omitempty"`
}

// PromotedNotifier is invoked after blocked convoys clear so callers can issue
// their proceed signal. Implementations must not block the arbiter.
type PromotedNotifier func(context.Context, model.TransitCycle)

// Service applies shared-reach arbitration and persists every grant/release.
type Service struct {
	arbiter    *Arbiter
	recorder   *journal.Recorder
	now        func() time.Time
	mu         sync.Mutex
	onPromoted PromotedNotifier
	// plans remembers each transit's convoy for its later exit request.
	plans map[requestKey]model.ConvoyPlan
}

func NewService(arbiter *Arbiter, recorder *journal.Recorder, now func() time.Time) *Service {
	if now == nil {
		now = time.Now
	}
	return &Service{arbiter: arbiter, recorder: recorder, now: now, plans: make(map[requestKey]model.ConvoyPlan)}
}

// OnPromoted registers the clearance callback.
func (s *Service) OnPromoted(notify PromotedNotifier) { s.onPromoted = notify }

func crossingRequest(cycle model.TransitCycle, plan model.ConvoyPlan, phase Phase) Request {
	waterway := EntryWaterway(cycle.Direction)
	if phase == PhaseExit {
		waterway = ExitWaterway(cycle.Direction)
	}
	return Request{
		Waterway:   waterway,
		ChamberID:  cycle.ChamberID,
		CycleID:    cycle.ID,
		PlanID:     plan.ID,
		Generation: cycle.Generation,
		Heading:    cycle.Direction,
		Vessels:    append([]model.Vessel(nil), plan.Vessels...),
	}
}

// RegisterPlan remembers the convoy behind a transit for its exit request.
func (s *Service) RegisterPlan(plan model.ConvoyPlan, cycle model.TransitCycle) {
	s.mu.Lock()
	s.plans[requestKey{chamberID: cycle.ChamberID, cycleID: cycle.ID}] = plan.Clone()
	s.mu.Unlock()
}

// AcquireEntry registers (or confirms) a transit's entry-reach crossing. When
// the reach is contested the convoy is queued and Granted is false.
func (s *Service) AcquireEntry(ctx context.Context, cycle model.TransitCycle, plan model.ConvoyPlan) (Verdict, error) {
	s.RegisterPlan(plan, cycle)
	return s.acquire(ctx, crossingRequest(cycle, plan, PhaseEntry))
}

// AcquireExit requests the downstream/upstream exit reach before release.
func (s *Service) AcquireExit(ctx context.Context, cycle model.TransitCycle) (Verdict, error) {
	plan, ok := s.planFor(cycle)
	if !ok {
		return Verdict{}, errMissingPlan
	}
	return s.acquire(ctx, crossingRequest(cycle, plan, PhaseExit))
}

func (s *Service) acquire(ctx context.Context, req Request) (Verdict, error) {
	outcome, err := s.arbiter.Acquire(req)
	if err != nil {
		return Verdict{}, err
	}
	if outcome.Granted {
		if err := s.recordAcquired(ctx, req); err != nil {
			return Verdict{}, err
		}
	}
	if err := s.recordPromotions(ctx, outcome.Promoted); err != nil {
		return Verdict{}, err
	}
	s.notifyPromoted(ctx, outcome.Promoted)
	return Verdict{Granted: outcome.Granted, Reason: outcome.Reason}, nil
}

// ReleasePhase frees one leg of a transit (used after the convoy is sealed in
// the chamber or after field confirmation of its departure).
func (s *Service) ReleasePhase(ctx context.Context, chamberID string, cycleID uuid.UUID, generation uint64, phase Phase, reason string) error {
	cleared, promoted := s.arbiter.Release(chamberID, cycleID)
	if err := s.recordReleases(ctx, chamberID, cycleID, generation, phase, reason, cleared); err != nil {
		return err
	}
	if err := s.recordPromotions(ctx, promoted); err != nil {
		return err
	}
	s.notifyPromoted(ctx, promoted)
	if phase == PhaseExit {
		s.forgetPlan(chamberID, cycleID)
	}
	return nil
}

// ReleaseCycle safely frees every crossing and waiter slot held by a transit
// (cancellation or emergency) and clears the next compatible convoys.
func (s *Service) ReleaseCycle(ctx context.Context, chamberID string, cycleID uuid.UUID, generation uint64, reason string) error {
	cleared, promoted := s.arbiter.Release(chamberID, cycleID)
	if err := s.recordReleases(ctx, chamberID, cycleID, generation, "", reason, cleared); err != nil {
		return err
	}
	if err := s.recordPromotions(ctx, promoted); err != nil {
		return err
	}
	s.notifyPromoted(ctx, promoted)
	s.forgetPlan(chamberID, cycleID)
	return nil
}

// ReleaseExit frees only the exit leg (used to compensate a rejected exit-gate
// command). It never touches an entry crossing.
func (s *Service) ReleaseExit(ctx context.Context, cycle model.TransitCycle, reason string) error {
	waterway := ExitWaterway(cycle.Direction)
	occupant, promoted, ok := s.arbiter.ReleaseWaterway(waterway, cycle.ChamberID, cycle.ID)
	if !ok {
		return nil
	}
	if err := s.recordReleases(ctx, cycle.ChamberID, cycle.ID, cycle.Generation, PhaseExit, reason, []Occupancy{occupant}); err != nil {
		return err
	}
	if err := s.recordPromotions(ctx, promoted); err != nil {
		return err
	}
	s.notifyPromoted(ctx, promoted)
	return nil
}

func (s *Service) recordReleases(ctx context.Context, chamberID string, cycleID uuid.UUID, generation uint64, phase Phase, reason string, cleared []Occupancy) error {
	for _, occupant := range cleared {
		clearedPhase := phase
		if clearedPhase == "" {
			clearedPhase = PhaseEntry
			if occupant.Request.Waterway == ExitWaterway(occupant.Request.Heading) {
				clearedPhase = PhaseExit
			}
		}
		record := ReleasedRecord{
			Waterway: occupant.Request.Waterway, Phase: clearedPhase, ChamberID: chamberID,
			CycleID: cycleID, Generation: generation, Reason: reason,
			ReleasedAt: s.now().UTC(),
		}
		if _, err := s.recorder.Record(ctx, chamberID, model.EventWaterwayReleased, cycleID, generation, record); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) recordAcquired(ctx context.Context, req Request) error {
	occupant := Occupancy{Request: cloneRequest(req), GrantedAt: s.now().UTC(), VesselCount: len(req.Vessels)}
	_, err := s.recorder.Record(ctx, req.ChamberID, model.EventWaterwayAcquired, req.CycleID, req.Generation, occupant)
	return err
}

func (s *Service) recordPromotions(ctx context.Context, promoted []Request) error {
	for _, req := range promoted {
		if err := s.recordAcquired(ctx, req); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) notifyPromoted(ctx context.Context, promoted []Request) {
	if len(promoted) == 0 {
		return
	}
	s.mu.Lock()
	notify := s.onPromoted
	s.mu.Unlock()
	if notify == nil {
		return
	}
	for _, req := range promoted {
		notify(ctx, model.TransitCycle{
			ID: req.CycleID, ChamberID: req.ChamberID, PlanID: req.PlanID,
			Direction: req.Heading, Generation: req.Generation,
		})
	}
}

// EntryVerdict reports whether a transit holds clearance for its approach leg.
func (s *Service) EntryVerdict(cycle model.TransitCycle) Verdict {
	plan, ok := s.planFor(cycle)
	if !ok {
		return Verdict{}
	}
	req := crossingRequest(cycle, plan, PhaseEntry)
	if s.arbiter.IsOccupant(req.Waterway, cycle.ChamberID, cycle.ID) {
		return Verdict{Granted: true}
	}
	return Verdict{Reason: s.arbiter.Blocked(req)}
}

func (s *Service) planFor(cycle model.TransitCycle) (model.ConvoyPlan, bool) {
	s.mu.Lock()
	plan, ok := s.plans[requestKey{chamberID: cycle.ChamberID, cycleID: cycle.ID}]
	s.mu.Unlock()
	return plan, ok
}

func (s *Service) forgetPlan(chamberID string, cycleID uuid.UUID) {
	s.mu.Lock()
	delete(s.plans, requestKey{chamberID: chamberID, cycleID: cycleID})
	s.mu.Unlock()
}

// restorePlans rebuilds convoy knowledge after journal replay.
func (s *Service) restorePlans(plans map[requestKey]model.ConvoyPlan) {
	s.mu.Lock()
	for key, plan := range plans {
		s.plans[key] = plan.Clone()
	}
	s.mu.Unlock()
}

// Views exposes the shared-reach arbitration board.
func (s *Service) Views() []WaterwayView { return s.arbiter.Views() }
