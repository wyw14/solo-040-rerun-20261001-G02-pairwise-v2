package waterspace

import "github.com/wyw14/cry-96/internal/model"

// WellKnownWaterways lists the shared pilotage reaches served by the lock.
func WellKnownWaterways() []string {
	return []string{string(model.DirectionUpstream), string(model.DirectionDownstream)}
}

// EntryWaterway is the shared approach reach a convoy crosses to enter the
// chamber. An upbound convoy enters from the downstream reach and vice versa.
func EntryWaterway(heading model.Direction) string {
	return string(heading.Opposite())
}

// ExitWaterway is the shared reach a convoy crosses after leaving the chamber.
func ExitWaterway(heading model.Direction) string {
	return string(heading)
}
