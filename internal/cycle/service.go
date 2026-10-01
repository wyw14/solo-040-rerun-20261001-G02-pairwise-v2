package cycle

import (
	"context"

	"github.com/wyw14/cry-96/internal/admission"
	"github.com/wyw14/cry-96/internal/model"
)

// Service drives an operator-visible transit through its safe boundaries.
type Service struct {
	coordinator *Coordinator
	admission   *admission.Service
}

func NewService(coordinator *Coordinator, admissionService *admission.Service) *Service {
	return &Service{coordinator: coordinator, admission: admissionService}
}

func (s *Service) Start(ctx context.Context, plan model.ConvoyPlan) (model.TransitCycle, error) {
	cycleValue, err := s.coordinator.Start(ctx, plan)
	if err != nil {
		return model.TransitCycle{}, err
	}
	decision, err := s.admission.Admit(ctx, cycleValue)
	if err != nil {
		return model.TransitCycle{}, err
	}
	if !decision.Allowed {
		// The transit is queued and waiting for shared-water clearance. It is
		// not an error: the convoy receives its proceed signal when the
		// arbiter promotes the request after the opposing convoy clears.
		return cycleValue, nil
	}
	return cycleValue, nil
}

func (s *Service) Current(chamberID string) (model.TransitCycle, bool) {
	return s.coordinator.Current(chamberID)
}
