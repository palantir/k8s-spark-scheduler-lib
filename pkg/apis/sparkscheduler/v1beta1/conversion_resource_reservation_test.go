// Copyright (c) 2021 Palantir Technologies. All rights reserved.
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

package v1beta1

import (
	"strconv"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/palantir/k8s-spark-scheduler-lib/pkg/apis/sparkscheduler"
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
		sparkscheduler.ReservationSpecAnnotationKey: v1Beta2JsonDriverReservation(1, 2, 3),
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

var v1Beta2ReservationWithGPUAndAdditionalExecutor = v1beta2.ResourceReservation{
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
			"executor": {
				Node: "test_node",
				Resources: v1beta2.ResourceList{
					string(v1beta2.ResourceCPU):    resource.NewQuantity(20, resource.DecimalSI),
					string(v1beta2.ResourceMemory): resource.NewQuantity(20, resource.BinarySI),
				},
			},
		}},
	Status: v1beta2.ResourceReservationStatus{Pods: map[string]string{
		"driver":   "test_driver",
		"executor": "test_executor",
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
	require.JSONEq(t, v1Beta1ReservationWithGPU.ObjectMeta.Annotations[sparkscheduler.ReservationSpecAnnotationKey], v1beta1ResConverted.ObjectMeta.Annotations[sparkscheduler.ReservationSpecAnnotationKey])
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

func TestConversionFromV2ToV1ToV2AfterAddingReservationsInV1(t *testing.T) {
	// We expect the final v1Beta2 struct to contain the changes that we made to the v1 struct as well as the gpu information
	// from the initial v2 object.

	// Convert to v1beta1
	var v1beta1ResConverted ResourceReservation
	err := v1beta1ResConverted.ConvertFrom(&v1Beta2ReservationWithGPU)
	if err != nil {
		t.Fatalf("Conversion from v1Beta2 to v1Beta1 failed with err: %s", err)
	}

	// Add reservation
	executorReservation := Reservation{
		Node:   "test_node",
		CPU:    *resource.NewQuantity(20, resource.DecimalSI),
		Memory: *resource.NewQuantity(20, resource.BinarySI),
	}
	v1beta1ResConverted.Spec.Reservations["executor"] = executorReservation
	v1beta1ResConverted.Status.Pods["executor"] = "test_executor"

	// Convert back to v1beta2
	var v1beta2ResConverted v1beta2.ResourceReservation
	err = v1beta1ResConverted.ConvertTo(&v1beta2ResConverted)
	if err != nil {
		t.Fatalf("Conversion from v1Beta1 to v1Beta2 failed with err: %s", err)
	}

	compareV1Beta2ResourceReservationSpecs(t, &v1Beta2ReservationWithGPUAndAdditionalExecutor.Spec, &v1beta2ResConverted.Spec)
	require.Equal(t, v1Beta2ReservationWithGPUAndAdditionalExecutor.Status, v1beta2ResConverted.Status)
	require.Empty(t, v1Beta2ReservationWithGPUAndAdditionalExecutor.ObjectMeta.Annotations)
}

func TestConversionFromV2ToV1ToV2AfterRemovingReservationsInV1(t *testing.T) {
	// We expect the final v1Beta2 struct to contain the changes that we made to the v1 struct as well as the gpu information
	// from the initial v2 object.

	// Convert to v1beta1
	var v1beta1ResConverted ResourceReservation
	err := v1beta1ResConverted.ConvertFrom(&v1Beta2ReservationWithGPUAndAdditionalExecutor)
	if err != nil {
		t.Fatalf("Conversion from v1Beta2 to v1Beta1 failed with err: %s", err)
	}

	// Remove executor reservation
	delete(v1beta1ResConverted.Spec.Reservations, "executor")
	delete(v1beta1ResConverted.Status.Pods, "executor")

	// Convert back to v1beta2
	var v1beta2ResConverted v1beta2.ResourceReservation
	err = v1beta1ResConverted.ConvertTo(&v1beta2ResConverted)
	if err != nil {
		t.Fatalf("Conversion from v1Beta1 to v1Beta2 failed with err: %s", err)
	}

	compareV1Beta2ResourceReservationSpecs(t, &v1Beta2ReservationWithGPU.Spec, &v1beta2ResConverted.Spec)
	require.Equal(t, v1Beta2ReservationWithGPU.Status, v1beta2ResConverted.Status)
	require.Empty(t, v1Beta2ReservationWithGPU.ObjectMeta.Annotations)
}

func compareV1Beta2ResourceReservationSpecs(t *testing.T, r1 *v1beta2.ResourceReservationSpec, r2 *v1beta2.ResourceReservationSpec) {
	if !cmp.Equal(r1, r2) {
		t.Fatalf("Resource reservation specs not equal: %s and %s", r1, r2)
	}
}

func v1Beta2JsonDriverReservation(cpu, memory, gpus int) string {
	return `{
			"reservations": {
				"driver": {
					"node": "test_node",
					"resources": {
						"` + string(v1beta2.ResourceCPU) + `": "` + strconv.Itoa(cpu) + `",
						"` + string(v1beta2.ResourceMemory) + `": "` + strconv.Itoa(memory) + `",
						"` + string(v1beta2.ResourceNvidiaGPU) + `": "` + strconv.Itoa(gpus) + `"
					}
				}
			}
		}`
}
