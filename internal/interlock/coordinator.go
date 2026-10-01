package interlock

import (
	"context"
	"fmt"
	"time"

	"github.com/wyw14/cry-96/internal/gate"
	"github.com/wyw14/cry-96/internal/model"
	"github.com/wyw14/cry-96/internal/waterspace"
)

// Clearance guards gate-open commands with shared pilotage-reach arbitration.
type Clearance interface {
	EntryVerdict(model.TransitCycle) waterspace.Verdict
	AcquireExit(context.Context, model.TransitCycle) (waterspace.Verdict, error)
	ReleaseExit(context.Context, model.TransitCycle, string) error
}

// Coordinator keeps policy evaluation and command reservation in one flow.
type Coordinator struct {
	policy    Policy
	arbiter   *Arbiter
	gates     *gate.Controller
	level     func(string) float64
	emergency func(string) bool
	clearance Clearance
}

func NewCoordinator(policy Policy, arbiter *Arbiter, gates *gate.Controller, level func(string) float64, emergency func(string) bool) *Coordinator {
	return &Coordinator{policy: policy, arbiter: arbiter, gates: gates, level: level, emergency: emergency}
}

// BindClearance attaches shared-water arbitration after the graph is wired.
func (c *Coordinator) BindClearance(clearance Clearance) { c.clearance = clearance }

func (c *Coordinator) Snapshot(chamberID string) EquipmentSnapshot {
	level := 0.0
	if c.level != nil {
		level = c.level(chamberID)
	}
	emergency := false
	if c.emergency != nil {
		emergency = c.emergency(chamberID)
	}
	return EquipmentSnapshot{
		ChamberID: chamberID, Gates: c.gates.State().Snapshot(chamberID),
		LevelM: level, Emergency: emergency,
	}
}

func (c *Coordinator) RequestOpen(ctx context.Context, cycle model.TransitCycle, direction model.Direction, owner string) (model.DeviceCommand, error) {
	if err := c.policy.ValidateOpen(c.Snapshot(cycle.ChamberID), direction); err != nil {
		return model.DeviceCommand{}, err
	}
	exitGate := direction == cycle.Direction
	if c.clearance != nil {
		// Entry gate: the convoy must already hold its approach reach. This is
		// a read-only check; the exit leg is acquired atomically below only if
		// the open command is actually issued.
		if !exitGate {
			verdict := c.clearance.EntryVerdict(cycle)
			if !verdict.Granted {
				reason := verdict.Reason
				if reason == "" {
					reason = "approach waterway clearance not held"
				}
				return model.DeviceCommand{}, fmt.Errorf("entry waterway blocked: %s", reason)
			}
		}
	}
	var command model.DeviceCommand
	_, err := c.arbiter.Reserve(cycle.ChamberID, direction, owner, func() error {
		acquiredExit := false
		if c.clearance != nil && exitGate {
			verdict, acquireErr := c.clearance.AcquireExit(ctx, cycle)
			if acquireErr != nil {
				return acquireErr
			}
			if !verdict.Granted {
				return fmt.Errorf("exit waterway blocked: %s", verdict.Reason)
			}
			acquiredExit = true
		}
		issued, issueErr := c.gates.Request(ctx, cycle, direction, true)
		if issueErr != nil && acquiredExit {
			// Compensate: do not hold shared water when no command was issued.
			_ = c.clearance.ReleaseExit(context.WithoutCancel(ctx), cycle, "gate command rejected after exit clearance")
			return issueErr
		}
		command = issued
		return issueErr
	})
	return command, err
}

func (c *Coordinator) RequestClose(ctx context.Context, cycle model.TransitCycle, direction model.Direction) (model.DeviceCommand, error) {
	if err := c.policy.ValidateClose(c.Snapshot(cycle.ChamberID), direction); err != nil {
		return model.DeviceCommand{}, err
	}
	return c.gates.Request(ctx, cycle, direction, false)
}

func (c *Coordinator) Release(chamberID, owner string) error {
	if !c.arbiter.Release(chamberID, owner) {
		return fmt.Errorf("reservation owner %q does not control chamber %s", owner, chamberID)
	}
	return nil
}

func (c *Coordinator) Reservation(chamberID string) (Reservation, bool) {
	return c.arbiter.Current(chamberID)
}

func (c *Coordinator) Observe(chamberID string, direction model.Direction, position model.GatePosition, _ time.Time) error {
	return c.gates.Observe(chamberID, direction, position)
}
