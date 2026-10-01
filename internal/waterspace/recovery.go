package waterspace

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/journal"
	"github.com/wyw14/cry-96/internal/model"
)

// Recovery rebuilds shared-reach crossings from every chamber's journal.
// Waterway events are partitioned per chamber, so each partition is replayed
// independently and the resulting crossings are merged by reach. It also
// re-queues convoys that were waiting for clearance when the process stopped,
// so in-flight occupancy and pending requests both survive a restart.
type Recovery struct {
	store   journal.Store
	service *Service
}

func NewRecovery(store journal.Store, service *Service) *Recovery {
	return &Recovery{store: store, service: service}
}

// RestoreAll replays waterway events and returns the crossings still in flight.
func (r *Recovery) RestoreAll(ctx context.Context) (map[string][]Occupancy, error) {
	chambers, err := r.store.Chambers(ctx)
	if err != nil {
		return nil, err
	}
	active := make(map[string]map[requestKey]Occupancy)
	plans := make(map[requestKey]model.ConvoyPlan)
	planByID := make(map[uuid.UUID]model.ConvoyPlan)
	latestCycle := make(map[requestKey]model.TransitCycle)

	for _, chamberID := range chambers {
		events, err := r.store.Events(ctx, chamberID, 0)
		if err != nil {
			return nil, err
		}
		for _, event := range events {
			if event.ChamberID != chamberID {
				return nil, fmt.Errorf("waterspace event partition mismatch chamber=%s event=%s", chamberID, event.ChamberID)
			}
			switch event.Type {
			case model.EventPlanCreated:
				if plan, ok := decodePlan(event.Payload); ok {
					planByID[plan.ID] = plan
				}
			case model.EventCycleCreated, model.EventCycleAdvanced:
				var cycleValue model.TransitCycle
				if err := event.Decode(&cycleValue); err != nil {
					return nil, err
				}
				latestCycle[requestKey{chamberID: chamberID, cycleID: cycleValue.ID}] = cycleValue
			case model.EventWaterwayAcquired:
				var occupant Occupancy
				if err := event.Decode(&occupant); err != nil {
					return nil, err
				}
				if occupant.Request.Waterway == "" {
					return nil, fmt.Errorf("waterspace acquire event %s missing waterway", event.ID)
				}
				occupant.Request.ChamberID = chamberID
				if active[occupant.Request.Waterway] == nil {
					active[occupant.Request.Waterway] = make(map[requestKey]Occupancy)
				}
				active[occupant.Request.Waterway][occupant.Request.key()] = occupant
				if _, known := plans[occupant.Request.key()]; !known {
					plans[occupant.Request.key()] = model.ConvoyPlan{
						ID:        occupant.Request.PlanID,
						ChamberID: chamberID,
						Direction: occupant.Request.Heading,
						Vessels:   append([]model.Vessel(nil), occupant.Request.Vessels...),
						CreatedAt: occupant.GrantedAt,
					}
				}
			case model.EventWaterwayReleased:
				var record ReleasedRecord
				if err := event.Decode(&record); err != nil {
					return nil, err
				}
				key := requestKey{chamberID: chamberID, cycleID: record.CycleID}
				if crossings := active[record.Waterway]; crossings != nil {
					delete(crossings, key)
					if len(crossings) == 0 {
						delete(active, record.Waterway)
					}
				}
				if record.Phase == PhaseExit {
					delete(plans, key)
				}
			}
		}
	}

	restored := make(map[string][]Occupancy, len(active))
	held := make(map[requestKey]bool)
	for waterway, crossings := range active {
		for key, occupant := range crossings {
			restored[waterway] = append(restored[waterway], occupant)
			held[key] = true
		}
	}
	r.service.arbiter.Restore(restored)

	// Re-queue convoys interrupted while waiting to enter or while releasing.
	// Going through the service records an acquired event for any request the
	// reach can already admit after restart and re-triggers promotion.
	for key, cycleValue := range latestCycle {
		if cycleValue.Stage.Terminal() || held[key] {
			continue
		}
		plan, ok := planByID[cycleValue.PlanID]
		if !ok {
			continue
		}
		switch cycleValue.Stage {
		case model.StagePlanned, model.StageAdmitting:
			if _, err := r.service.AcquireEntry(ctx, cycleValue, plan); err != nil {
				return nil, err
			}
			plans[key] = plan
		case model.StageReleasing:
			if _, err := r.service.AcquireExit(ctx, cycleValue); err != nil {
				return nil, err
			}
			plans[key] = plan
		}
	}

	r.service.restorePlans(plans)
	return restored, nil
}

// decodePlan accepts the full ConvoyPlan payload the planner records; the
// admission queue's plan.created event carries a smaller map and is ignored.
func decodePlan(payload json.RawMessage) (model.ConvoyPlan, bool) {
	if len(payload) == 0 {
		return model.ConvoyPlan{}, false
	}
	var plan model.ConvoyPlan
	if err := json.Unmarshal(payload, &plan); err != nil || plan.ID == uuid.Nil || len(plan.Vessels) == 0 {
		return model.ConvoyPlan{}, false
	}
	return plan, true
}
