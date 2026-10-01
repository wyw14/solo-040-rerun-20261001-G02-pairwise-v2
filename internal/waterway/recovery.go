package waterway

import (
	"context"
	"fmt"

	"github.com/wyw14/cry-96/internal/journal"
	"github.com/wyw14/cry-96/internal/model"
)

// Recovery rebuilds active waterway occupations from the journal so in-transit
// occupations are not lost when the process restarts.
type Recovery struct {
	replayer *journal.Replayer
	service  *Service
}

func NewRecovery(replayer *journal.Replayer, service *Service) *Recovery {
	return &Recovery{replayer: replayer, service: service}
}

// RestoreAll replays every chamber partition and returns the occupations that
// are still active after applying all acquire/release events in order.
func (r *Recovery) RestoreAll(ctx context.Context) ([]model.WaterwayOccupation, error) {
	states, err := r.replayer.ReplayAll(ctx)
	if err != nil {
		return nil, err
	}
	for chamberID, state := range states {
		for _, event := range state.Applied {
			if event.Type != model.EventWaterwayAcquired && event.Type != model.EventWaterwayReleased {
				continue
			}
			var occupation model.WaterwayOccupation
			if err := event.Decode(&occupation); err != nil {
				return nil, err
			}
			if occupation.ChamberID != chamberID {
				return nil, fmt.Errorf("waterway occupation partition mismatch chamber=%s occupation=%s", chamberID, occupation.ChamberID)
			}
			r.service.Restore(occupation)
		}
	}
	return r.service.Active(), nil
}
