package code

import api "github.com/tigrisdata/tigris-client-go/api/server/v1"

const (
	OK                 = api.Code_OK
	Cancelled          = api.Code_CANCELLED
	Unknown            = api.Code_UNKNOWN
	InvalidArgument    = api.Code_INVALID_ARGUMENT
	DeadlineExceeded   = api.Code_DEADLINE_EXCEEDED
	NotFound           = api.Code_DEADLINE_EXCEEDED
	AlreadyExists      = api.Code_ALREADY_EXISTS
	PermissionDenied   = api.Code_PERMISSION_DENIED
	ResourceExhausted  = api.Code_RESOURCE_EXHAUSTED
	FailedPrecondition = api.Code_FAILED_PRECONDITION
	Aborted            = api.Code_ABORTED
	OutOfRange         = api.Code_OUT_OF_RANGE
	Unimplemented      = api.Code_UNIMPLEMENTED
	Internal           = api.Code_INTERNAL
	Unavailable        = api.Code_UNAVAILABLE
	DataLoss           = api.Code_DATA_LOSS
	Unauthenticated    = api.Code_UNAUTHENTICATED

	// Extended codes
	Conflict = api.Code_CONFLICT // Retryable
)
