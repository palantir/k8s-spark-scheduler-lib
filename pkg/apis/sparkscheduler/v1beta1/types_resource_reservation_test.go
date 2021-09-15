// Copyright (c) 2019 Palantir Technologies. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// These tests primarily make sure that we can handle cases where the object we get from the
// API server does not have an NvidiaGPU field set. It also tests standard marshalling and unmarshalling.
package v1beta1

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/resource"
)

var (
	reservationWithGPUJson = []byte(`
	{
		"node": "test_node",
		"cpu": "1",
		"memory": "100",
		"nvidia.com/gpu": "1"
	}
	`)
	expectedReservationWithGPU = Reservation{
		Node:      "test_node",
		CPU:       *resource.NewQuantity(1, resource.DecimalSI),
		Memory:    *resource.NewQuantity(100, resource.DecimalSI),
		NvidiaGPU: *resource.NewQuantity(1, resource.DecimalSI),
	}
	reservationWithoutGPUJson = []byte(`
	{
		"node": "test_node",
		"cpu": "1",
		"memory": "100"
	}
	`)
	reservationWithZeroGPUJson = []byte(`
	{
		"node": "test_node",
		"cpu": "1",
		"memory": "100",
		"nvidia.com/gpu": "0"
	}
	`)
	expectedReservationWithoutGPU = Reservation{
		Node:      "test_node",
		CPU:       *resource.NewQuantity(1, resource.DecimalSI),
		Memory:    *resource.NewQuantity(100, resource.DecimalSI),
		NvidiaGPU: *resource.NewQuantity(0, resource.DecimalSI),
	}
)

func cacheStringValuesOfReservation(r *Reservation) {
	_ = r.CPU.String()
	_ = r.Memory.String()
	_ = r.NvidiaGPU.String()
}

func compareReservations(t *testing.T, r1 *Reservation, r2 *Reservation) {
	// Calling string caches the string value of the quantity, unmarshalled reservations already have this so we need
	// to call it for all reservations to get deep equality to be consistent
	cacheStringValuesOfReservation(r1)
	cacheStringValuesOfReservation(r2)
	require.Equal(t, r1, r2)
}

func TestJsonUnmarshalReservation(t *testing.T) {
	var unmarshalledReservation Reservation
	err := json.Unmarshal(reservationWithGPUJson, &unmarshalledReservation)
	if err != nil {
		t.Fatalf("Failed to unmarshal valid unmarshalled reservation. %s", err)
	}
	compareReservations(t, &unmarshalledReservation, &expectedReservationWithGPU)
}

func TestJsonMarshalReservation(t *testing.T) {
	bytes, err := json.Marshal(&expectedReservationWithGPU)
	if err != nil {
		t.Fatalf("Failed to marshal valid reservation. %s", err)
	}
	require.JSONEq(t, string(reservationWithGPUJson), string(bytes))
}

func TestJsonUnmarshalReservationWithoutGpu(t *testing.T) {
	var unmarshalledReservation Reservation
	err := json.Unmarshal(reservationWithoutGPUJson, &unmarshalledReservation)
	if err != nil {
		t.Fatalf("Failed to unmarshal valid unmarshalled_reservation. %s", err)
	}
	compareReservations(t, &unmarshalledReservation, &expectedReservationWithoutGPU)
}

func TestJsonMarshalReservationWithoutGpu(t *testing.T) {
	bytes, err := json.Marshal(&expectedReservationWithoutGPU)
	if err != nil {
		t.Fatalf("Failed to marshal valid reservation. %s", err)
	}
	require.JSONEq(t, string(reservationWithZeroGPUJson), string(bytes))
}
