package cycle_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/wyw14/cry-96/internal/admission"
	"github.com/wyw14/cry-96/internal/cycle"
	"github.com/wyw14/cry-96/internal/gate"
	"github.com/wyw14/cry-96/internal/hydraulics"
	"github.com/wyw14/cry-96/internal/journal"
	"github.com/wyw14/cry-96/internal/model"
	"github.com/wyw14/cry-96/internal/signal"
	"github.com/wyw14/cry-96/internal/waterway"
)

type testRig struct {
	coordinator *cycle.Coordinator
	service     *cycle.Service
	admission   *admission.Service
	waterway    *waterway.Service
	gates       *gate.Controller
	signals     *signal.Store
}

func newTestRig() *testRig {
	store := journal.NewMemoryStore()
	recorder := journal.NewRecorder(store, time.Now)
	gateState := gate.NewState("chamber-1", "chamber-2")
	commands := gate.NewCommands()
	gates := gate.NewController(gateState, commands, recorder, time.Now)
	ramp := hydraulics.NewRamp(time.Millisecond, time.Now)
	hydraulicsController := hydraulics.NewController(commands, recorder, ramp, time.Now)
	signalStore := signal.NewStore()
	admissionQueue := admission.NewQueue()
	admissionController := admission.NewController(admissionQueue, gateState, signalStore, time.Now)
	admissionService := admission.NewService(admissionController, recorder, time.Now)
	waterwayService := waterway.NewService(recorder, time.Now)
	coordinator := cycle.NewCoordinator(gates, hydraulicsController, recorder, time.Now)
	coordinator.BindWaterway(waterwayService)
	return &testRig{
		coordinator: coordinator,
		service:     cycle.NewService(coordinator, admissionService, waterwayService),
		admission:   admissionService,
		waterway:    waterwayService, gates: gates, signals: signalStore,
	}
}

func startCycle(t *testing.T, rig *testRig, chamberID string, direction model.Direction, vesselName string) model.TransitCycle {
	t.Helper()
	ctx := context.Background()
	plan := testPlan(t, chamberID, direction, vesselName)
	if err := rig.admission.Enqueue(ctx, plan); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	cycleValue, err := rig.service.Start(ctx, plan)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	return cycleValue
}

func testPlan(t *testing.T, chamberID string, direction model.Direction, vesselName string) model.ConvoyPlan {
	t.Helper()
	vessel, err := model.NewVessel(vesselName, "RS-1", 86)
	if err != nil {
		t.Fatalf("vessel: %v", err)
	}
	plan, err := model.NewConvoyPlan(chamberID, direction, []model.Vessel{vessel}, time.Now())
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	return plan
}

func ackGate(t *testing.T, rig *testRig, cycleValue model.TransitCycle, direction model.Direction, open bool) {
	t.Helper()
	ctx := context.Background()
	command, err := rig.gates.Request(ctx, cycleValue, direction, open)
	if err != nil {
		t.Fatalf("gate request: %v", err)
	}
	ack := model.DeviceAck{
		CommandID: command.ID, CycleID: command.CycleID, ChamberID: command.ChamberID,
		Generation: command.Generation, Epoch: command.Epoch, Device: command.Device,
		Settled: true, ObservedAt: time.Now().UTC(),
	}
	if err := rig.coordinator.ApplyGateAck(ctx, ack); err != nil {
		t.Fatalf("gate ack: %v", err)
	}
}

func TestStartAcquiresEntryOccupation(t *testing.T) {
	rig := newTestRig()
	startCycle(t, rig, "chamber-1", model.DirectionUpstream, "River Star")
	active := rig.waterway.Active()
	if len(active) != 1 || active[0].Phase != model.WaterwayEntry || active[0].Fleet[0] != "River Star" {
		t.Fatalf("active occupations = %+v", active)
	}
}

func TestStartRejectsConflictingDirection(t *testing.T) {
	rig := newTestRig()
	ctx := context.Background()
	startCycle(t, rig, "chamber-1", model.DirectionUpstream, "River Star")
	opposing := testPlan(t, "chamber-2", model.DirectionDownstream, "Ocean Wind")
	if err := rig.admission.Enqueue(ctx, opposing); err != nil {
		t.Fatalf("enqueue opposing: %v", err)
	}
	_, err := rig.service.Start(ctx, opposing)
	var conflict *waterway.ConflictError
	if !errors.As(err, &conflict) {
		t.Fatalf("expected ConflictError, got %v", err)
	}
	// The conflicting fleet must not have received a proceed signal.
	for _, vessel := range opposing.Vessels {
		if state, ok := rig.signals.Get(vessel.ID); ok {
			t.Fatalf("conflicting vessel got signal %s", state.Aspect)
		}
	}
	// The refused attempt must not wedge chamber-2 behind a zombie transit.
	current, ok := rig.coordinator.Current("chamber-2")
	if !ok || !current.Stage.Terminal() {
		t.Fatalf("chamber-2 cycle = %+v, want terminal", current)
	}
}

func TestSealedReleasesEntryOccupation(t *testing.T) {
	rig := newTestRig()
	cycleValue := startCycle(t, rig, "chamber-1", model.DirectionUpstream, "River Star")
	ackGate(t, rig, cycleValue, model.DirectionDownstream, true) // planned -> admitting
	if got := len(rig.waterway.Active()); got != 1 {
		t.Fatalf("entry hold must survive admitting, got %d", got)
	}
	ackGate(t, rig, cycleValue, model.DirectionDownstream, false) // admitting -> sealed
	if got := len(rig.waterway.Active()); got != 0 {
		t.Fatalf("entry hold must be released once sealed, got %d", got)
	}
}

func TestCancelReleasesOccupations(t *testing.T) {
	rig := newTestRig()
	ctx := context.Background()
	startCycle(t, rig, "chamber-1", model.DirectionUpstream, "River Star")
	if _, err := rig.coordinator.Cancel(ctx, "chamber-1", "operator cancellation"); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	if got := len(rig.waterway.Active()); got != 0 {
		t.Fatalf("cancel must release occupations, got %d", got)
	}
}

func TestEmergencyReleasesOccupations(t *testing.T) {
	rig := newTestRig()
	ctx := context.Background()
	startCycle(t, rig, "chamber-1", model.DirectionUpstream, "River Star")
	if _, err := rig.coordinator.Cancel(ctx, "chamber-1", "emergency"); err != nil {
		t.Fatalf("emergency cancel: %v", err)
	}
	if got := len(rig.waterway.Active()); got != 0 {
		t.Fatalf("emergency must release occupations, got %d", got)
	}
}

func TestConfirmedDepartureReleasesExitOccupation(t *testing.T) {
	rig := newTestRig()
	ctx := context.Background()
	cycleValue := startCycle(t, rig, "chamber-1", model.DirectionUpstream, "River Star")
	ackGate(t, rig, cycleValue, model.DirectionDownstream, true)  // planned -> admitting
	ackGate(t, rig, cycleValue, model.DirectionDownstream, false) // admitting -> sealed
	ackGate(t, rig, cycleValue, model.DirectionUpstream, false)   // sealed -> leveling
	if _, err := rig.waterway.Acquire(ctx, cycleValue, model.WaterwayExit, []string{"River Star"}); err != nil {
		t.Fatalf("acquire exit: %v", err)
	}
	ackGate(t, rig, cycleValue, model.DirectionUpstream, true) // leveling -> releasing
	if got := len(rig.waterway.Active()); got != 1 {
		t.Fatalf("exit hold must survive releasing, got %d", got)
	}
	if err := rig.coordinator.ApplySignalHandoff(ctx, cycleValue.ID, cycleValue.Generation); err != nil {
		t.Fatalf("signal handoff: %v", err)
	}
	if got := len(rig.waterway.Active()); got != 0 {
		t.Fatalf("confirmed departure must release occupations, got %d", got)
	}
}
