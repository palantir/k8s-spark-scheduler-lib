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

// These tests test conversion between v1beta1 and v1beta2 of the resource reservation CRD.
package v1beta1

import (
	"testing"

	"github.com/palantir/k8s-spark-scheduler-lib/pkg/apis/sparkscheduler/v1beta2"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/resource"
)

var v1Beta1ReservationWithGPU = ResourceReservation{
	Spec: ResourceReservationSpec{Reservations: map[string]Reservation{
		"driver": {
			Node:      "test_node",
			CPU:       *resource.NewQuantity(1, resource.DecimalSI),
			Memory:    *resource.NewQuantity(2, resource.DecimalSI),
			NvidiaGPU: *resource.NewQuantity(3, resource.DecimalSI),
		},
	}},
	Status: ResourceReservationStatus{Pods: map[string]string{
		"driver": "test_driver",
	}},
}

var v1Beta1ReservationWithoutGPU = ResourceReservation{
	Spec: ResourceReservationSpec{Reservations: map[string]Reservation{
		"driver": {
			Node:   "test_node",
			CPU:    *resource.NewQuantity(1, resource.DecimalSI),
			Memory: *resource.NewQuantity(2, resource.DecimalSI),
		},
	}},
	Status: ResourceReservationStatus{Pods: map[string]string{
		"driver": "test_driver",
	}},
}

var v1Beta1ReservationWithZeroGPU = ResourceReservation{
	Spec: ResourceReservationSpec{Reservations: map[string]Reservation{
		"driver": {
			Node:      "test_node",
			CPU:       *resource.NewQuantity(1, resource.DecimalSI),
			Memory:    *resource.NewQuantity(2, resource.DecimalSI),
			NvidiaGPU: *resource.NewQuantity(0, resource.DecimalSI),
		},
	}},
	Status: ResourceReservationStatus{Pods: map[string]string{
		"driver": "test_driver",
	}},
}

var v1Beta2ReservationWithGPU = v1beta2.ResourceReservation{
	Spec: v1beta2.ResourceReservationSpec{Reservations: map[string]v1beta2.Reservation{
		"driver": {
			Node:      "test_node",
			CPU:       *resource.NewQuantity(1, resource.DecimalSI),
			Memory:    *resource.NewQuantity(2, resource.DecimalSI),
			NvidiaGPU: *resource.NewQuantity(3, resource.DecimalSI),
		},
	}},
	Status: v1beta2.ResourceReservationStatus{Pods: map[string]string{
		"driver": "test_driver",
	}},
}

var v1Beta2ReservationWithZeroGPU = v1beta2.ResourceReservation{
	Spec: v1beta2.ResourceReservationSpec{Reservations: map[string]v1beta2.Reservation{
		"driver": {
			Node:      "test_node",
			CPU:       *resource.NewQuantity(1, resource.DecimalSI),
			Memory:    *resource.NewQuantity(2, resource.DecimalSI),
			NvidiaGPU: *resource.NewQuantity(0, resource.DecimalSI),
		},
	}},
	Status: v1beta2.ResourceReservationStatus{Pods: map[string]string{
		"driver": "test_driver",
	}},
}

func TestConversionFromV1Beta2ToV1Beta1WithGPUs(t *testing.T) {
	// The general case, we expect the new v1Beta1 struct to have the same fields and values as the v1beta2 struct
	var v1beta1ResConverted ResourceReservation
	err := v1beta1ResConverted.ConvertFrom(&v1Beta2ReservationWithGPU)
	if err != nil {
		t.Fatalf("Conversion from v1Beta2 to v1Beta1 failed with err: %s", err)
	}
	require.Equal(t, v1Beta1ReservationWithGPU, v1beta1ResConverted)
}

func TestConversionFromV1Beta1ToV1Beta2WithGPUs(t *testing.T) {
	// The general case, we expect the new v1Beta2 struct to have the same fields and values as the v1beta1 struct
	var v1beta2ResConverted v1beta2.ResourceReservation
	err := v1Beta1ReservationWithGPU.ConvertTo(&v1beta2ResConverted)
	if err != nil {
		t.Fatalf("Conversion from v1Beta2 to v1Beta1 failed with err: %s", err)
	}
	require.Equal(t, v1Beta2ReservationWithGPU, v1beta2ResConverted)
}

func TestConversionFromV1Beta1ToV1Beta2WithoutGPUs(t *testing.T) {
	// The case where the v1beta1 struct has no value set for NvidiaGPU, in this case we expect to create a v1beta2
	// struct with 0 NvidiaGPUs
	var v1beta2ResConverted v1beta2.ResourceReservation
	err := v1Beta1ReservationWithoutGPU.ConvertTo(&v1beta2ResConverted)
	if err != nil {
		t.Fatalf("Conversion from v1Beta2 to v1Beta1 failed with err: %s", err)
	}
	require.Equal(t, v1Beta2ReservationWithZeroGPU, v1beta2ResConverted)
}

func TestConversionFromV1Beta2ToV1Beta1WithNoGPUs(t *testing.T) {
	// The case where the v1beta2 struct has zero gpus. In this case we just expect to v1beta1 struct to have the same
	// values as the v1beta2 struct. We DO NOT expect the v1beta1 object to have nil for the NvidiaGPU field.
	var v1beta1ResConverted ResourceReservation
	err := v1beta1ResConverted.ConvertFrom(&v1Beta2ReservationWithZeroGPU)
	if err != nil {
		t.Fatalf("Conversion from v1Beta2 to v1Beta1 failed with err: %s", err)
	}
	require.Equal(t, v1Beta1ReservationWithZeroGPU, v1beta1ResConverted)
}
