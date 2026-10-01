package waterway_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/journal"
	"github.com/wyw14/cry-96/internal/model"
	"github.com/wyw14/cry-96/internal/waterway"
)

func newTestService() (*waterway.Service, journal.Store) {
	store := journal.NewMemoryStore()
	recorder := journal.NewRecorder(store, time.Now)
	return waterway.NewService(recorder, time.Now), store
}

func testCycle(chamberID string, direction model.Direction) model.TransitCycle {
	now := time.Now().UTC()
	return model.TransitCycle{
		ID: uuid.New(), ChamberID: chamberID, PlanID: uuid.New(),
		Direction: direction, Stage: model.StageAdmitting, Generation: 1,
		CreatedAt: now, UpdatedAt: now,
	}
}

func TestAcquireRejectsOppositeDirection(t *testing.T) {
	service, _ := newTestService()
	ctx := context.Background()
	upstream := testCycle("chamber-1", model.DirectionUpstream)
	if _, err := service.Acquire(ctx, upstream, model.WaterwayEntry, []string{"River Star"}); err != nil {
		t.Fatalf("acquire upstream: %v", err)
	}
	downstream := testCycle("chamber-2", model.DirectionDownstream)
	_, err := service.Acquire(ctx, downstream, model.WaterwayEntry, []string{"Ocean Wind"})
	var conflict *waterway.ConflictError
	if !errors.As(err, &conflict) {
		t.Fatalf("expected ConflictError, got %v", err)
	}
	if len(conflict.Blockers) != 1 || conflict.Blockers[0].Fleet[0] != "River Star" {
		t.Fatalf("blockers = %+v", conflict.Blockers)
	}
	if !strings.Contains(err.Error(), "River Star") {
		t.Fatalf("error should name the blocking fleet: %v", err)
	}
}

func TestAcquireAllowsSameDirection(t *testing.T) {
	service, _ := newTestService()
	ctx := context.Background()
	first := testCycle("chamber-1", model.DirectionUpstream)
	second := testCycle("chamber-2", model.DirectionUpstream)
	if _, err := service.Acquire(ctx, first, model.WaterwayEntry, []string{"River Star"}); err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	if _, err := service.Acquire(ctx, second, model.WaterwayExit, []string{"Ocean Wind"}); err != nil {
		t.Fatalf("same-direction acquire must be allowed: %v", err)
	}
	if got := len(service.Active()); got != 2 {
		t.Fatalf("active occupations = %d, want 2", got)
	}
}

func TestAcquireIsIdempotentPerCyclePhase(t *testing.T) {
	service, _ := newTestService()
	ctx := context.Background()
	cycleValue := testCycle("chamber-1", model.DirectionUpstream)
	first, err := service.Acquire(ctx, cycleValue, model.WaterwayEntry, []string{"River Star"})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	second, err := service.Acquire(ctx, cycleValue, model.WaterwayEntry, []string{"River Star"})
	if err != nil {
		t.Fatalf("re-acquire: %v", err)
	}
	if first.ID != second.ID {
		t.Fatalf("idempotent acquire returned %s then %s", first.ID, second.ID)
	}
	if got := len(service.Active()); got != 1 {
		t.Fatalf("active occupations = %d, want 1", got)
	}
}

func TestReleasePhaseFreesOpposingTraffic(t *testing.T) {
	service, _ := newTestService()
	ctx := context.Background()
	upstream := testCycle("chamber-1", model.DirectionUpstream)
	if _, err := service.Acquire(ctx, upstream, model.WaterwayEntry, []string{"River Star"}); err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if err := service.ReleasePhase(ctx, upstream, model.WaterwayEntry, "fleet sealed in chamber"); err != nil {
		t.Fatalf("release: %v", err)
	}
	downstream := testCycle("chamber-2", model.DirectionDownstream)
	if _, err := service.Acquire(ctx, downstream, model.WaterwayEntry, []string{"Ocean Wind"}); err != nil {
		t.Fatalf("opposing acquire after release: %v", err)
	}
}

func TestReleaseAllClearsEveryPhase(t *testing.T) {
	service, _ := newTestService()
	ctx := context.Background()
	cycleValue := testCycle("chamber-1", model.DirectionUpstream)
	if _, err := service.Acquire(ctx, cycleValue, model.WaterwayEntry, []string{"River Star"}); err != nil {
		t.Fatalf("acquire entry: %v", err)
	}
	if _, err := service.Acquire(ctx, cycleValue, model.WaterwayExit, []string{"River Star"}); err != nil {
		t.Fatalf("acquire exit: %v", err)
	}
	if err := service.ReleaseAll(ctx, cycleValue, "transit cancelled"); err != nil {
		t.Fatalf("release all: %v", err)
	}
	if got := len(service.Active()); got != 0 {
		t.Fatalf("active occupations = %d, want 0", got)
	}
}

func TestReleaseIsIdempotent(t *testing.T) {
	service, _ := newTestService()
	ctx := context.Background()
	cycleValue := testCycle("chamber-1", model.DirectionUpstream)
	if err := service.ReleasePhase(ctx, cycleValue, model.WaterwayEntry, "not held"); err != nil {
		t.Fatalf("releasing an unheld phase must be a no-op: %v", err)
	}
	if err := service.ReleaseAll(ctx, cycleValue, "not held"); err != nil {
		t.Fatalf("release all with nothing held must be a no-op: %v", err)
	}
}

func TestTerminalCycleCannotAcquire(t *testing.T) {
	service, _ := newTestService()
	cycleValue := testCycle("chamber-1", model.DirectionUpstream)
	cycleValue.Stage = model.StageCompleted
	if _, err := service.Acquire(context.Background(), cycleValue, model.WaterwayExit, []string{"River Star"}); err == nil {
		t.Fatal("terminal transit must not occupy the waterway")
	}
}

func TestStatusReportsBlockersPerDirection(t *testing.T) {
	service, _ := newTestService()
	ctx := context.Background()
	upstream := testCycle("chamber-1", model.DirectionUpstream)
	if _, err := service.Acquire(ctx, upstream, model.WaterwayExit, []string{"River Star"}); err != nil {
		t.Fatalf("acquire: %v", err)
	}
	status := service.Status()
	if len(status.Occupations) != 1 {
		t.Fatalf("occupations = %+v", status.Occupations)
	}
	byDirection := map[model.Direction]bool{}
	blockers := map[model.Direction]int{}
	for _, entry := range status.Directions {
		byDirection[entry.Direction] = entry.Blocked
		blockers[entry.Direction] = len(entry.Blockers)
	}
	if byDirection[model.DirectionUpstream] {
		t.Fatal("upstream traffic must stay clear while an upstream hold is active")
	}
	if !byDirection[model.DirectionDownstream] || blockers[model.DirectionDownstream] != 1 {
		t.Fatalf("downstream must be blocked with one blocker: %+v", status.Directions)
	}
}

func TestRecoveryRestoresInTransitOccupations(t *testing.T) {
	store := journal.NewMemoryStore()
	recorder := journal.NewRecorder(store, time.Now)
	ctx := context.Background()
	first := waterway.NewService(recorder, time.Now)
	kept := testCycle("chamber-1", model.DirectionUpstream)
	if _, err := first.Acquire(ctx, kept, model.WaterwayExit, []string{"River Star"}); err != nil {
		t.Fatalf("acquire kept: %v", err)
	}
	departed := testCycle("chamber-2", model.DirectionUpstream)
	if _, err := first.Acquire(ctx, departed, model.WaterwayEntry, []string{"Ocean Wind"}); err != nil {
		t.Fatalf("acquire departed: %v", err)
	}
	if err := first.ReleasePhase(ctx, departed, model.WaterwayEntry, "fleet sealed in chamber"); err != nil {
		t.Fatalf("release departed: %v", err)
	}

	// Restart: a fresh service over the same journal must see the in-transit hold.
	restarted := waterway.NewService(recorder, time.Now)
	recovery := waterway.NewRecovery(journal.NewReplayer(store), restarted)
	active, err := recovery.RestoreAll(ctx)
	if err != nil {
		t.Fatalf("restore: %v", err)
	}
	if len(active) != 1 || active[0].CycleID != kept.ID || active[0].Phase != model.WaterwayExit {
		t.Fatalf("restored occupations = %+v", active)
	}
	if blockers := restarted.Blockers(model.DirectionDownstream); len(blockers) != 1 {
		t.Fatalf("restored occupation must still block opposing traffic: %+v", blockers)
	}
	if _, err := restarted.Acquire(ctx, testCycle("chamber-2", model.DirectionDownstream), model.WaterwayEntry, []string{"Ocean Wind"}); err == nil {
		t.Fatal("conflicting acquire after restart must be rejected")
	}
}
