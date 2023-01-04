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

package api

import (
	"context"

	"github.com/grpc-ecosystem/go-grpc-middleware/util/metautils"
	"google.golang.org/grpc"
)

var (
	HeaderPrefix   = "Tigris-"
	HeaderTxID     = HeaderPrefix + "Tx-Id"
	HeaderTxOrigin = HeaderPrefix + "Tx-Origin"

	grpcGatewayPrefix = "grpc-gateway-"
)

func IsTxSupported(ctx context.Context) bool {
	m, _ := grpc.Method(ctx)
	switch m {
	case "Insert", "Replace", "Update", "Delete", "Read",
		"CreateOrUpdateCollection", "DropCollection", "ListCollections":
		return true
	default:
		return false
	}
}

func getHeader(ctx context.Context, header string) string {
	if val := metautils.ExtractIncoming(ctx).Get(header); val != "" {
		return val
	}

	return metautils.ExtractIncoming(ctx).Get(grpcGatewayPrefix + header)
}

func GetTransaction(ctx context.Context) *TransactionCtx {
	tx := &TransactionCtx{
		Id:     getHeader(ctx, HeaderTxID),
		Origin: getHeader(ctx, HeaderTxOrigin),
	}

	if tx.Id != "" {
		return tx
	}

	return nil
}
