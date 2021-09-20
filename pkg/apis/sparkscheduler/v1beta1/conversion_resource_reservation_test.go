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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var v1Beta1ReservationWithGPU = ResourceReservation{
	Spec: ResourceReservationSpec{Reservations: map[string]Reservation{
		"driver": {
			Node:   "test_node",
			CPU:    *resource.NewQuantity(1, resource.DecimalSI),
			Memory: *resource.NewQuantity(2, resource.BinarySI),
		},
	}},
	Status: ResourceReservationStatus{Pods: map[string]string{
		"driver": "test_driver",
	}},
	ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{
		ReservationsAnnotation: `{
			"reservations": {
				"driver": {
					"node": "test_node",
					"resources": {
						"` + string(v1beta2.ResourceCPU) + `": "1",
						"` + string(v1beta2.ResourceMemory) + `": "2",
						"` + string(v1beta2.ResourceNvidiaGPU) + `": "3"
					}
				}
			}
		}`,
	}},
}

var v1Beta1ReservationWithoutGPU = ResourceReservation{
	Spec: ResourceReservationSpec{Reservations: map[string]Reservation{
		"driver": {
			Node:   "test_node",
			CPU:    *resource.NewQuantity(1, resource.DecimalSI),
			Memory: *resource.NewQuantity(2, resource.BinarySI),
		},
	}},
	Status: ResourceReservationStatus{Pods: map[string]string{
		"driver": "test_driver",
	}},
}

var v1Beta2ReservationWithGPU = v1beta2.ResourceReservation{
	Spec: v1beta2.ResourceReservationSpec{
		Reservations: map[string]v1beta2.Reservation{
			"driver": {
				Node: "test_node",
				Resources: v1beta2.ResourceList{
					string(v1beta2.ResourceCPU):       resource.NewQuantity(1, resource.DecimalSI),
					string(v1beta2.ResourceMemory):    resource.NewQuantity(2, resource.BinarySI),
					string(v1beta2.ResourceNvidiaGPU): resource.NewQuantity(3, resource.DecimalSI),
				},
			},
		}},
	Status: v1beta2.ResourceReservationStatus{Pods: map[string]string{
		"driver": "test_driver",
	}},
}

var v1Beta2ReservationWithGPUAndPreConversionChanges = v1beta2.ResourceReservation{
	Spec: v1beta2.ResourceReservationSpec{
		Reservations: map[string]v1beta2.Reservation{
			"driver": {
				Node: "test_node",
				Resources: v1beta2.ResourceList{
					string(v1beta2.ResourceCPU):       resource.NewQuantity(10, resource.DecimalSI),
					string(v1beta2.ResourceMemory):    resource.NewQuantity(2, resource.BinarySI),
					string(v1beta2.ResourceNvidiaGPU): resource.NewQuantity(3, resource.DecimalSI),
				},
			},
		}},
	Status: v1beta2.ResourceReservationStatus{Pods: map[string]string{
		"driver": "test_driver",
	}},
}

var v1Beta2ReservationWithoutGPU = v1beta2.ResourceReservation{
	Spec: v1beta2.ResourceReservationSpec{
		Reservations: map[string]v1beta2.Reservation{
			"driver": {
				Node: "test_node",
				Resources: v1beta2.ResourceList{
					string(v1beta2.ResourceCPU):    resource.NewQuantity(1, resource.DecimalSI),
					string(v1beta2.ResourceMemory): resource.NewQuantity(2, resource.BinarySI),
				},
			},
		}},
	Status: v1beta2.ResourceReservationStatus{Pods: map[string]string{
		"driver": "test_driver",
	}},
}

func TestConversionFromV1Beta2ToV1Beta1WithGPUs(t *testing.T) {
	// We expect the new v1Beta1 struct to have CPU and memory set with the v1Beta2 reservation spec in annotations
	var v1beta1ResConverted ResourceReservation
	err := v1beta1ResConverted.ConvertFrom(&v1Beta2ReservationWithGPU)
	if err != nil {
		t.Fatalf("Conversion from v1Beta2 to v1Beta1 failed with err: %s", err)
	}
	require.Equal(t, v1Beta1ReservationWithGPU.Spec, v1beta1ResConverted.Spec)
	require.Equal(t, v1Beta1ReservationWithGPU.Status, v1beta1ResConverted.Status)
	require.JSONEq(t, v1Beta1ReservationWithGPU.ObjectMeta.Annotations[ReservationsAnnotation], v1beta1ResConverted.ObjectMeta.Annotations[ReservationsAnnotation])
}

func TestConversionFromV1Beta1ToV1Beta2WithGPUs(t *testing.T) {
	// We expect the v1Beta2 struct to have CPU, Memory AND GPU set correctly after fetching all values
	// from the annotation
	var v1beta2ResConverted v1beta2.ResourceReservation
	err := v1Beta1ReservationWithGPU.ConvertTo(&v1beta2ResConverted)
	if err != nil {
		t.Fatalf("Conversion from v1Beta1 to v1Beta2 failed with err: %s", err)
	}
	compareV1Beta2ResourceReservationSpecs(t, &v1Beta2ReservationWithGPU.Spec, &v1beta2ResConverted.Spec)
	require.Equal(t, v1Beta2ReservationWithGPU.Status, v1beta2ResConverted.Status)
	require.Empty(t, v1beta2ResConverted.ObjectMeta.Annotations)
}

func TestConversionFromV1Beta1ToV1Beta2WithoutGPUs(t *testing.T) {
	// We expect the v1Beta2 struct to only have CPU and Memory set after fetching the values from the v1beta1
	// resource reservation spec directly
	var v1beta2ResConverted v1beta2.ResourceReservation
	err := v1Beta1ReservationWithoutGPU.ConvertTo(&v1beta2ResConverted)
	if err != nil {
		t.Fatalf("Conversion from v1Beta1 to v1Beta2 failed with err: %s", err)
	}
	compareV1Beta2ResourceReservationSpecs(t, &v1Beta2ReservationWithoutGPU.Spec, &v1beta2ResConverted.Spec)
	require.Equal(t, v1Beta2ReservationWithoutGPU.Status, v1beta2ResConverted.Status)
	require.Empty(t, v1beta2ResConverted.ObjectMeta.Annotations)
}

func TestConversionFromV2ToV1ToV2AfterChangingValuesInV1(t *testing.T) {
	// We expect the final v1Beta2 struct to contain the changes that we made to the v1 struct as well as the gpu information
	// from the initial v2 object.

	// Convert to v1beta1
	var v1beta1ResConverted ResourceReservation
	err := v1beta1ResConverted.ConvertFrom(&v1Beta2ReservationWithGPU)
	if err != nil {
		t.Fatalf("Conversion from v1Beta2 to v1Beta1 failed with err: %s", err)
	}

	// Change the reservation object
	reservation, _ := v1beta1ResConverted.Spec.Reservations["driver"]
	reservation.CPU = *resource.NewQuantity(10, resource.DecimalSI)
	v1beta1ResConverted.Spec.Reservations["driver"] = reservation

	// Convert back to v1beta2
	var v1beta2ResConverted v1beta2.ResourceReservation
	err = v1beta1ResConverted.ConvertTo(&v1beta2ResConverted)
	if err != nil {
		t.Fatalf("Conversion from v1Beta1 to v1Beta2 failed with err: %s", err)
	}

	compareV1Beta2ResourceReservationSpecs(t, &v1Beta2ReservationWithGPUAndPreConversionChanges.Spec, &v1beta2ResConverted.Spec)
	require.Equal(t, v1Beta2ReservationWithGPUAndPreConversionChanges.Status, v1beta2ResConverted.Status)
	require.Empty(t, v1Beta2ReservationWithGPUAndPreConversionChanges.ObjectMeta.Annotations)
}

func cacheStringValuesOfReservations(r *v1beta2.ResourceReservationSpec) {
	// Calling String() caches the string value of the quantity, unmarshalled reservations already have this so we need
	// to call it for all reservations to get deep equality to be consistent
	for _, value := range r.Reservations {
		_ = value.Resources.CPU().String()
		_ = value.Resources.Memory().String()
		_ = value.Resources.NvidiaGPU().String()
	}
}

func compareV1Beta2ResourceReservationSpecs(t *testing.T, r1 *v1beta2.ResourceReservationSpec, r2 *v1beta2.ResourceReservationSpec) {
	cacheStringValuesOfReservations(r1)
	cacheStringValuesOfReservations(r2)
	require.Equal(t, r1, r2)
}
