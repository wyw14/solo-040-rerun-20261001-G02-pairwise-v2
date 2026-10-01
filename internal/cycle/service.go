package cycle

import (
	"context"
	"errors"

	"github.com/wyw14/cry-96/internal/admission"
	"github.com/wyw14/cry-96/internal/model"
)

// WaterwayArbiter grants the shared waterway to a transit by direction.
type WaterwayArbiter interface {
	Acquire(ctx context.Context, cycle model.TransitCycle, phase model.WaterwayPhase, fleet []string) (model.WaterwayOccupation, error)
	ReleasePhase(ctx context.Context, cycle model.TransitCycle, phase model.WaterwayPhase, reason string) error
}

// Service drives an operator-visible transit through its safe boundaries.
type Service struct {
	coordinator *Coordinator
	admission   *admission.Service
	waterway    WaterwayArbiter
}

func NewService(coordinator *Coordinator, admissionService *admission.Service, waterway WaterwayArbiter) *Service {
	return &Service{coordinator: coordinator, admission: admissionService, waterway: waterway}
}

func (s *Service) Start(ctx context.Context, plan model.ConvoyPlan) (model.TransitCycle, error) {
	cycleValue, err := s.coordinator.Start(ctx, plan)
	if err != nil {
		return model.TransitCycle{}, err
	}
	if s.waterway != nil {
		// The fleet must hold the shared waterway before admission may grant
		// the proceed signal; opposing headings are refused here.
		if _, err := s.waterway.Acquire(ctx, cycleValue, model.WaterwayEntry, fleetNames(plan)); err != nil {
			// Roll back the just-created cycle so a refused attempt does not
			// wedge the chamber behind a zombie planned transit.
			_, _ = s.coordinator.Cancel(context.WithoutCancel(ctx), plan.ChamberID, "waterway conflict")
			return model.TransitCycle{}, err
		}
	}
	decision, err := s.admission.Admit(ctx, cycleValue)
	if err != nil {
		s.releaseEntry(ctx, cycleValue, "admission failed")
		return model.TransitCycle{}, err
	}
	if !decision.Allowed {
		s.releaseEntry(ctx, cycleValue, "admission denied: "+decision.Reason)
		return cycleValue, errors.New(decision.Reason)
	}
	return cycleValue, nil
}

func (s *Service) releaseEntry(ctx context.Context, cycleValue model.TransitCycle, reason string) {
	if s.waterway != nil {
		_ = s.waterway.ReleasePhase(context.WithoutCancel(ctx), cycleValue, model.WaterwayEntry, reason)
	}
}

func fleetNames(plan model.ConvoyPlan) []string {
	names := make([]string, 0, len(plan.Vessels))
	for _, vessel := range plan.Vessels {
		names = append(names, vessel.Name)
	}
	return names
}

func (s *Service) Current(chamberID string) (model.TransitCycle, bool) {
	return s.coordinator.Current(chamberID)
}
