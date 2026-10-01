package waterspace

import (
	"context"
	"testing"
	"time"

	"github.com/wyw14/cry-96/internal/journal"
	"github.com/wyw14/cry-96/internal/model"
)

func testVessel() model.Vessel {
	vessel, _ := model.NewVessel("River Star", "RS-1", 80)
	return vessel
}

func testCycle(chamberID string, direction model.Direction) (model.ConvoyPlan, model.TransitCycle) {
	now := time.Unix(1_700_000_000, 0).UTC()
	plan, _ := model.NewConvoyPlan(chamberID, direction, []model.Vessel{testVessel()}, now)
	cycleValue, _ := model.NewTransitCycle(chamberID, plan.ID, direction, 1, now)
	return plan, cycleValue
}

func recordPlanAndCycle(t *testing.T, ctx context.Context, recorder *journal.Recorder, plan model.ConvoyPlan, cycleValue model.TransitCycle) {
	t.Helper()
	if _, err := recorder.Record(ctx, plan.ChamberID, model.EventPlanCreated, plan.ID, 0, plan); err != nil {
		t.Fatal(err)
	}
	if _, err := recorder.Record(ctx, cycleValue.ChamberID, model.EventCycleCreated, cycleValue.ID, cycleValue.Generation, cycleValue); err != nil {
		t.Fatal(err)
	}
}

func makeReq(waterway, chamberID string, heading model.Direction) Request {
	plan, cycleValue := testCycle(chamberID, heading)
	return Request{
		Waterway: waterway, ChamberID: chamberID, CycleID: cycleValue.ID, PlanID: plan.ID,
		Generation: 1, Heading: heading, Vessels: plan.Vessels,
	}
}

func TestArbiterOpposingHeadingsBlockAndPromote(t *testing.T) {
	arbiter := NewArbiter(time.Now)
	upbound := makeReq(string(model.DirectionUpstream), "chamber-1", model.DirectionUpstream)
	downbound := makeReq(string(model.DirectionUpstream), "chamber-2", model.DirectionDownstream)

	outcome, err := arbiter.Acquire(upbound)
	if err != nil || !outcome.Granted {
		t.Fatalf("upbound entry should be granted: granted=%v err=%v", outcome.Granted, err)
	}
	outcome, err = arbiter.Acquire(downbound)
	if err != nil {
		t.Fatalf("blocked acquire must not error: %v", err)
	}
	if outcome.Granted || outcome.Reason == "" {
		t.Fatalf("opposing heading must be blocked with a reason, got granted=%v reason=%q", outcome.Granted, outcome.Reason)
	}
	if reason := arbiter.Blocked(downbound); reason == "" {
		t.Fatal("Blocked must report the opposing occupant")
	}

	cleared, promoted := arbiter.Release("chamber-1", upbound.CycleID)
	if len(cleared) != 1 || len(promoted) != 1 {
		t.Fatalf("release should clear 1 and promote 1, got %d/%d", len(cleared), len(promoted))
	}
	if promoted[0].CycleID != downbound.CycleID {
		t.Fatal("the waiting downbound convoy must be promoted")
	}
	if !arbiter.IsOccupant(string(model.DirectionUpstream), "chamber-2", downbound.CycleID) {
		t.Fatal("promoted convoy must now occupy the reach")
	}
}

func TestArbiterSameHeadingSharesReach(t *testing.T) {
	arbiter := NewArbiter(time.Now)
	first := makeReq("downstream", "chamber-1", model.DirectionUpstream)
	second := makeReq("downstream", "chamber-2", model.DirectionUpstream)
	if outcome, _ := arbiter.Acquire(first); !outcome.Granted {
		t.Fatal("first same-heading convoy should be granted")
	}
	if outcome, _ := arbiter.Acquire(second); !outcome.Granted {
		t.Fatal("following same-heading convoy must share the reach")
	}
}

func TestArbiterFIFODoesNotSkipOpposingHead(t *testing.T) {
	arbiter := NewArbiter(time.Now)
	holder := makeReq("upstream", "chamber-1", model.DirectionUpstream)
	waiterOpposed := makeReq("upstream", "chamber-2", model.DirectionDownstream)
	waiterSame := makeReq("upstream", "chamber-1", model.DirectionUpstream)
	arbiter.Acquire(holder)
	arbiter.Acquire(waiterOpposed)
	// holder still present, so this queues behind the opposing head.
	outcome, _ := arbiter.Acquire(waiterSame)
	if outcome.Granted {
		t.Fatal("same-heading convoy must not jump an opposing FIFO head")
	}
	_, promoted := arbiter.Release("chamber-1", holder.CycleID)
	if len(promoted) != 1 || promoted[0].CycleID != waiterOpposed.CycleID {
		t.Fatalf("only the opposing head should be promoted, got %d", len(promoted))
	}
}

func TestServiceAcquireReleaseAndRecovery(t *testing.T) {
	ctx := context.Background()
	store := journal.NewMemoryStore()
	recorder := journal.NewRecorder(store, time.Now)
	service := NewService(NewArbiter(time.Now), recorder, time.Now)

	plan, cycleValue := testCycle("chamber-1", model.DirectionUpstream)
	verdict, err := service.AcquireEntry(ctx, cycleValue, plan)
	if err != nil || !verdict.Granted {
		t.Fatalf("entry acquire failed: granted=%v err=%v", verdict.Granted, err)
	}

	// Fresh process: rebuild in-flight occupancy from the durable journal.
	rebuilt := NewService(NewArbiter(time.Now), recorder, time.Now)
	if _, err := NewRecovery(store, rebuilt).RestoreAll(ctx); err != nil {
		t.Fatalf("restore failed: %v", err)
	}
	if occupants := countOccupants(rebuilt); occupants != 1 {
		t.Fatalf("in-flight occupancy must survive restart, got %d occupants", occupants)
	}

	// Continue the transit: seal (release entry), acquire then clear exit.
	if err := rebuilt.ReleasePhase(ctx, "chamber-1", cycleValue.ID, 1, PhaseEntry, "sealed"); err != nil {
		t.Fatalf("release entry: %v", err)
	}
	if v, err := rebuilt.AcquireExit(ctx, cycleValue); err != nil || !v.Granted {
		t.Fatalf("exit acquire: %v %v", v, err)
	}
	if err := rebuilt.ReleasePhase(ctx, "chamber-1", cycleValue.ID, 1, PhaseExit, "departure confirmed"); err != nil {
		t.Fatalf("release exit: %v", err)
	}

	final := NewService(NewArbiter(time.Now), recorder, time.Now)
	if _, err := NewRecovery(store, final).RestoreAll(ctx); err != nil {
		t.Fatalf("final restore failed: %v", err)
	}
	if occupants := countOccupants(final); occupants != 0 {
		t.Fatalf("all reaches should be clear after release, got %d occupants", occupants)
	}
}

func countOccupants(service *Service) int {
	var total int
	for _, view := range service.Views() {
		total += len(view.Occupants)
	}
	return total
}

func TestRecoveryRequeuesWaitingConvoy(t *testing.T) {
	ctx := context.Background()
	store := journal.NewMemoryStore()
	recorder := journal.NewRecorder(store, time.Now)
	service := NewService(NewArbiter(time.Now), recorder, time.Now)

	// chamber-1 upbound holds the upstream reach at its exit leg.
	holderPlan, holderCycle := testCycle("chamber-1", model.DirectionUpstream)
	recordPlanAndCycle(t, ctx, recorder, holderPlan, holderCycle)
	if _, err := service.AcquireEntry(ctx, holderCycle, holderPlan); err != nil {
		t.Fatal(err)
	}
	if err := service.ReleasePhase(ctx, "chamber-1", holderCycle.ID, 1, PhaseEntry, "sealed"); err != nil {
		t.Fatal(err)
	}
	if v, err := service.AcquireExit(ctx, holderCycle); err != nil || !v.Granted {
		t.Fatalf("holder exit acquire: %v %v", v, err)
	}

	// chamber-2 downbound wants to enter via the upstream reach -> blocked.
	waiterPlan, waiterCycle := testCycle("chamber-2", model.DirectionDownstream)
	recordPlanAndCycle(t, ctx, recorder, waiterPlan, waiterCycle)
	v, err := service.AcquireEntry(ctx, waiterCycle, waiterPlan)
	if err != nil || v.Granted {
		t.Fatalf("waiter should be blocked, got %v err=%v", v, err)
	}

	// Restart: holder crossing restored, waiter re-queued with reason.
	rebuilt := NewService(NewArbiter(time.Now), recorder, time.Now)
	if _, err := NewRecovery(store, rebuilt).RestoreAll(ctx); err != nil {
		t.Fatal(err)
	}
	var waiting int
	for _, view := range rebuilt.Views() {
		waiting += len(view.Waiting)
		if view.Waterway == "upstream" {
			if len(view.Occupants) != 1 {
				t.Fatalf("holder occupancy must survive restart, got %d", len(view.Occupants))
			}
		}
	}
	if waiting != 1 {
		t.Fatalf("blocked convoy must survive restart as waiting, got %d", waiting)
	}
}

func TestRequestValidation(t *testing.T) {
	arbiter := NewArbiter(time.Now)
	if _, err := arbiter.Acquire(Request{Waterway: "upstream"}); err == nil {
		t.Fatal("incomplete request must be rejected")
	}
}
