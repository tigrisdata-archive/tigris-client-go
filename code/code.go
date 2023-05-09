// Copyright 2022-2023 Tigris Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package code

import api "github.com/tigrisdata/tigris-client-go/api/server/v1"

const (
	OK                 = api.Code_OK
	Cancelled          = api.Code_CANCELLED
	Unknown            = api.Code_UNKNOWN
	InvalidArgument    = api.Code_INVALID_ARGUMENT
	DeadlineExceeded   = api.Code_DEADLINE_EXCEEDED
	NotFound           = api.Code_NOT_FOUND
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

	// Extended codes.
	Conflict = api.Code_CONFLICT // Retryable
)

type Code = api.Code
