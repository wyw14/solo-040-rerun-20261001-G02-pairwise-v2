package waterspace

import (
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/model"
)

// Request describes one convoy's request to cross a shared pilotage reach.
type Request struct {
	Waterway   string          `json:"waterway"`
	ChamberID  string          `json:"chamber_id"`
	CycleID    uuid.UUID       `json:"cycle_id"`
	PlanID     uuid.UUID       `json:"plan_id"`
	Generation uint64          `json:"generation"`
	Heading    model.Direction `json:"heading"`
	Vessels    []model.Vessel  `json:"vessels"`
}

func (r Request) validate() error {
	if r.Waterway == "" || r.ChamberID == "" || r.CycleID == uuid.Nil ||
		r.PlanID == uuid.Nil || r.Generation == 0 || !r.Heading.Valid() || len(r.Vessels) == 0 {
		return errors.New("waterspace request requires waterway, chamber, cycle, plan, generation, heading and vessels")
	}
	return nil
}

func (r Request) key() requestKey {
	return requestKey{chamberID: r.ChamberID, cycleID: r.CycleID}
}

type requestKey struct {
	chamberID string
	cycleID   uuid.UUID
}

// Occupancy is one crossing currently cleared inside a shared reach.
type Occupancy struct {
	Request     Request   `json:"request"`
	GrantedAt   time.Time `json:"granted_at"`
	VesselCount int       `json:"vessel_count"`
}

// WaitingConvoy is a blocked request with the reason it cannot be cleared.
type WaitingConvoy struct {
	Request Request   `json:"request"`
	Since   time.Time `json:"since"`
	Reason  string    `json:"reason"`
}

// WaterwayView is the operator-facing state of one shared reach.
type WaterwayView struct {
	Waterway  string          `json:"waterway"`
	Heading   model.Direction `json:"heading,omitempty"`
	Occupants []Occupancy     `json:"occupants"`
	Waiting   []WaitingConvoy `json:"waiting"`
	ClearToGo bool            `json:"clear_to_go"`
	UpdatedAt time.Time       `json:"updated_at"`
}

type reachState struct {
	heading   model.Direction
	crossings map[requestKey]Occupancy
	waiters   []WaitingConvoy
}

func newReachState() *reachState {
	return &reachState{crossings: make(map[requestKey]Occupancy)}
}

// Arbiter mutually excludes conflicting headings inside each shared reach.
// Convoys on the same heading may share a reach; an opposing heading waits
// until every crossing of the current heading has cleared.
type Arbiter struct {
	mu      sync.RWMutex
	reaches map[string]*reachState
	now     func() time.Time
}

func NewArbiter(now func() time.Time) *Arbiter {
	if now == nil {
		now = time.Now
	}
	arbiter := &Arbiter{reaches: make(map[string]*reachState), now: now}
	for _, waterway := range WellKnownWaterways() {
		arbiter.reaches[waterway] = newReachState()
	}
	return arbiter
}

func (a *Arbiter) reach(waterway string) *reachState {
	state := a.reaches[waterway]
	if state == nil {
		state = newReachState()
		a.reaches[waterway] = state
	}
	return state
}

// AcquireOutcome reports whether the request was granted and any new grants it
// caused (Promoted are waiters that the release-style promotion cleared).
type AcquireOutcome struct {
	Granted  bool
	Reason   string
	Promoted []Request
}

// Acquire grants a crossing immediately or queues the convoy as a waiter.
// Re-requesting an existing occupant or queued waiter is idempotent; a queued
// waiter is promoted as soon as the reach admits its heading.
func (a *Arbiter) Acquire(req Request) (AcquireOutcome, error) {
	if err := req.validate(); err != nil {
		return AcquireOutcome{}, err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	state := a.reach(req.Waterway)
	key := req.key()
	if _, ok := state.crossings[key]; ok {
		return AcquireOutcome{Granted: true}, nil
	}
	if index := waiterIndex(state, key); index >= 0 {
		state.waiters[index].Request = cloneRequest(req)
		if a.canCrossLocked(state, req) {
			return a.promoteWaiterLocked(state, index)
		}
		state.waiters[index].Reason = a.blockingReasonLocked(state, req)
		return AcquireOutcome{Granted: false, Reason: state.waiters[index].Reason}, nil
	}
	// Never jump an existing FIFO waiter, even when the heading would share.
	if a.canCrossLocked(state, req) && len(state.waiters) == 0 {
		a.grantLocked(state, req)
		return AcquireOutcome{Granted: true}, nil
	}
	state.waiters = append(state.waiters, WaitingConvoy{
		Request: cloneRequest(req), Since: a.now().UTC(),
		Reason: a.blockingReasonLocked(state, req),
	})
	if state.waiters[len(state.waiters)-1].Reason == "" {
		state.waiters[len(state.waiters)-1].Reason = "queued behind waiting convoy"
	}
	return AcquireOutcome{Granted: false, Reason: state.waiters[len(state.waiters)-1].Reason}, nil
}

// Release frees every crossing (or waiter slot) held by a transit and promotes
// the waiters the reach can now admit. It returns the cleared occupancies and
// the newly promoted requests.
func (a *Arbiter) Release(chamberID string, cycleID uuid.UUID) (cleared []Occupancy, promoted []Request) {
	a.mu.Lock()
	defer a.mu.Unlock()
	key := requestKey{chamberID: chamberID, cycleID: cycleID}
	for _, state := range a.reaches {
		if occupant, ok := state.crossings[key]; ok {
			delete(state.crossings, key)
			cleared = append(cleared, occupant)
			if len(state.crossings) == 0 {
				state.heading = ""
			}
		}
		if index := waiterIndex(state, key); index >= 0 {
			state.waiters = append(state.waiters[:index], state.waiters[index+1:]...)
		}
		a.promoteReadyLocked(state, &promoted)
		a.refreshWaiterReasonsLocked(state)
	}
	return cleared, promoted
}

// ReleaseWaterway frees one crossing of a specific reach and promotes any
// waiters the reach can now admit.
func (a *Arbiter) ReleaseWaterway(waterway, chamberID string, cycleID uuid.UUID) (Occupancy, []Request, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	state := a.reach(waterway)
	key := requestKey{chamberID: chamberID, cycleID: cycleID}
	occupant, ok := state.crossings[key]
	if !ok {
		return Occupancy{}, nil, false
	}
	delete(state.crossings, key)
	if len(state.crossings) == 0 {
		state.heading = ""
	}
	var promoted []Request
	a.promoteReadyLocked(state, &promoted)
	a.refreshWaiterReasonsLocked(state)
	return occupant, promoted, true
}

func (a *Arbiter) canCrossLocked(state *reachState, req Request) bool {
	return len(state.crossings) == 0 || state.heading == req.Heading
}

func (a *Arbiter) grantLocked(state *reachState, req Request) {
	if len(state.crossings) == 0 {
		state.heading = req.Heading
	}
	state.crossings[req.key()] = Occupancy{
		Request: cloneRequest(req), GrantedAt: a.now().UTC(), VesselCount: len(req.Vessels),
	}
}

func (a *Arbiter) promoteWaiterLocked(state *reachState, index int) (AcquireOutcome, error) {
	req := state.waiters[index].Request
	state.waiters = append(state.waiters[:index], state.waiters[index+1:]...)
	a.grantLocked(state, req)
	outcome := AcquireOutcome{Granted: true}
	outcome.Promoted = a.promoteFollowersLocked(state, req.Heading)
	outcome.Promoted = append([]Request{cloneRequest(req)}, outcome.Promoted...)
	return outcome, nil
}

// promoteFollowersLocked joins same-heading waiters that immediately follow.
func (a *Arbiter) promoteFollowersLocked(state *reachState, heading model.Direction) []Request {
	var promoted []Request
	for index := 0; index < len(state.waiters); {
		if state.waiters[index].Request.Heading != heading {
			break // FIFO: do not skip an opposing convoy at the head.
		}
		req := state.waiters[index].Request
		state.waiters = append(state.waiters[:index], state.waiters[index+1:]...)
		a.grantLocked(state, req)
		promoted = append(promoted, cloneRequest(req))
	}
	return promoted
}

func (a *Arbiter) promoteReadyLocked(state *reachState, promoted *[]Request) {
	if len(state.crossings) != 0 || len(state.waiters) == 0 {
		return
	}
	heading := state.waiters[0].Request.Heading
	for index := 0; index < len(state.waiters); {
		if state.waiters[index].Request.Heading != heading {
			break
		}
		req := state.waiters[index].Request
		state.waiters = append(state.waiters[:index], state.waiters[index+1:]...)
		a.grantLocked(state, req)
		*promoted = append(*promoted, cloneRequest(req))
	}
}

func (a *Arbiter) refreshWaiterReasonsLocked(state *reachState) {
	for index := range state.waiters {
		state.waiters[index].Reason = a.blockingReasonLocked(state, state.waiters[index].Request)
	}
}

func (a *Arbiter) blockingReasonLocked(state *reachState, req Request) string {
	if len(state.crossings) == 0 {
		return ""
	}
	if state.heading == req.Heading {
		return ""
	}
	for _, occupant := range state.crossings {
		return "shared reach " + occupant.Request.Waterway + " occupied by opposing " +
			string(state.heading) + " convoy from " + occupant.Request.ChamberID
	}
	return ""
}

func waiterIndex(state *reachState, key requestKey) int {
	for index := range state.waiters {
		if state.waiters[index].Request.key() == key {
			return index
		}
	}
	return -1
}

// Blocked reports why a transit cannot cross ("" when it already holds or may
// take the reach).
func (a *Arbiter) Blocked(req Request) string {
	a.mu.RLock()
	defer a.mu.RUnlock()
	state := a.reach(req.Waterway)
	if _, ok := state.crossings[req.key()]; ok {
		return ""
	}
	if a.canCrossLocked(state, req) {
		return ""
	}
	return a.blockingReasonLocked(state, req)
}

// IsOccupant reports whether the transit currently holds clearance for a reach.
func (a *Arbiter) IsOccupant(waterway, chamberID string, cycleID uuid.UUID) bool {
	a.mu.RLock()
	defer a.mu.RUnlock()
	state := a.reaches[waterway]
	_, ok := state.crossings[requestKey{chamberID: chamberID, cycleID: cycleID}]
	return ok
}

// Restore rebuilds crossings after replay without heading conflict checks.
func (a *Arbiter) Restore(active map[string][]Occupancy) {
	a.mu.Lock()
	defer a.mu.Unlock()
	for waterway, occupants := range active {
		state := a.reach(waterway)
		for _, occupant := range occupants {
			if len(state.crossings) == 0 {
				state.heading = occupant.Request.Heading
			}
			state.crossings[occupant.Request.key()] = occupant
		}
	}
}

// Views returns an operator-facing snapshot ordered by waterway name.
func (a *Arbiter) Views() []WaterwayView {
	a.mu.RLock()
	defer a.mu.RUnlock()
	names := make([]string, 0, len(a.reaches))
	for name := range a.reaches {
		names = append(names, name)
	}
	sort.Strings(names)
	result := make([]WaterwayView, 0, len(names))
	for _, name := range names {
		state := a.reaches[name]
		view := WaterwayView{
			Waterway: name, Heading: state.heading,
			ClearToGo: len(state.crossings) == 0, UpdatedAt: a.now().UTC(),
		}
		for _, occupant := range state.crossings {
			view.Occupants = append(view.Occupants, occupant)
		}
		sort.Slice(view.Occupants, func(i, j int) bool {
			return view.Occupants[i].GrantedAt.Before(view.Occupants[j].GrantedAt)
		})
		view.Waiting = append([]WaitingConvoy(nil), state.waiters...)
		result = append(result, view)
	}
	return result
}

func cloneRequest(req Request) Request {
	req.Vessels = append([]model.Vessel(nil), req.Vessels...)
	return req
}
