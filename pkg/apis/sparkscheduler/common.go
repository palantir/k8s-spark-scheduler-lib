package sparkscheduler

// Constants used across resource reservation versions

const (
	// ResourceReservationPlural defines how to refer to multiple resource reservations
	ResourceReservationPlural = "resourcereservations"

	// GroupName defines the kubernetes group name for resource reservations
	GroupName = "sparkscheduler.palantir.com"

	// ResourceReservationCRDName defines the fully qualified name of the CRD
	ResourceReservationCRDName = ResourceReservationPlural + "." + GroupName

	// ReservationSpecAnnotationKey is the field we set in the object annotation which holds the resource reservation spec
	// in objects with a version < latest version.
	// This is set so that we don't lose information in round trip conversions.
	ReservationSpecAnnotationKey = "scheduler.palantir.github.com/reservation-spec"
)
