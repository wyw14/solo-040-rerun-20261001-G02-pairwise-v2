package model

import (
	"errors"
	"time"

	"github.com/google/uuid"
)

// WaterwayPhase identifies which leg of a transit holds the shared waterway.
type WaterwayPhase string

const (
	// WaterwayEntry covers the fleet crossing the approach waterway into the chamber.
	WaterwayEntry WaterwayPhase = "entry"
	// WaterwayExit covers the fleet leaving the chamber into the shared waterway.
	WaterwayExit WaterwayPhase = "exit"
)

func (p WaterwayPhase) Valid() bool {
	return p == WaterwayEntry || p == WaterwayExit
}

// WaterwayOccupation is a granted hold on the shared pilotage waterway.
// Occupations of opposite directions must never be active at the same time.
type WaterwayOccupation struct {
	ID         uuid.UUID     `json:"id"`
	ChamberID  string        `json:"chamber_id"`
	CycleID    uuid.UUID     `json:"cycle_id"`
	Generation uint64        `json:"generation"`
	Direction  Direction     `json:"direction"`
	Phase      WaterwayPhase `json:"phase"`
	Fleet      []string      `json:"fleet"`
	Active     bool          `json:"active"`
	Reason     string        `json:"reason,omitempty"`
	AcquiredAt time.Time     `json:"acquired_at"`
	ReleasedAt *time.Time    `json:"released_at,omitempty"`
}

func NewWaterwayOccupation(chamberID string, cycleID uuid.UUID, generation uint64, direction Direction, phase WaterwayPhase, fleet []string, now time.Time) (WaterwayOccupation, error) {
	if chamberID == "" || cycleID == uuid.Nil || generation == 0 {
		return WaterwayOccupation{}, errors.New("occupation requires chamber, cycle and generation")
	}
	if !direction.Valid() {
		return WaterwayOccupation{}, errors.New("occupation direction is invalid")
	}
	if !phase.Valid() {
		return WaterwayOccupation{}, errors.New("occupation phase is invalid")
	}
	return WaterwayOccupation{
		ID: uuid.New(), ChamberID: chamberID, CycleID: cycleID, Generation: generation,
		Direction: direction, Phase: phase, Fleet: append([]string(nil), fleet...),
		Active: true, AcquiredAt: now.UTC(),
	}, nil
}

func (o WaterwayOccupation) Clone() WaterwayOccupation {
	o.Fleet = append([]string(nil), o.Fleet...)
	return o
}

// ConflictsWith reports whether this active occupation blocks a request for direction.
func (o WaterwayOccupation) ConflictsWith(direction Direction) bool {
	return o.Active && o.Direction.Valid() && o.Direction == direction.Opposite()
}

// Release marks the occupation inactive with the operator-visible reason.
func (o *WaterwayOccupation) Release(reason string, now time.Time) {
	o.Active = false
	o.Reason = reason
	released := now.UTC()
	o.ReleasedAt = &released
}
