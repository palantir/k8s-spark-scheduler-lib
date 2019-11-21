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

package binpack

import (
	"context"
	"reflect"
	"testing"

	"github.com/palantir/k8s-spark-scheduler-lib/pkg/resources"
)

func TestAzAwareTightlyPack(t *testing.T) {
	tests := []struct {
		name               string
		driverResources    *resources.Resources
		executorResources  *resources.Resources
		numExecutors       int
		availableResources resources.NodeGroupResources
		nodeZoneLabels     map[string]string
		nodePriorityOrder  []string
		expectedDriverNode string
		willFit            bool
		expectedCounts     map[string]int
	}{{
		name:              "picks the first zone when application fits entirely in either of the zones",
		driverResources:   createResources(1, 3),
		executorResources: createResources(2, 5),
		numExecutors:      2,
		availableResources: resources.NodeGroupResources(map[string]*resources.Resources{
			"n1_z1": createResources(4, 5),
			"n1_z2": createResources(4, 8),
			"n2_z1": createResources(6, 15),
			"n2_z2": createResources(6, 20),
		}),
		nodeZoneLabels:     map[string]string{"n1_z1": "z1", "n1_z2": "z2", "n2_z1": "z1", "n2_z2": "z2"},
		nodePriorityOrder:  []string{"n1_z1", "n1_z2", "n2_z1", "n2_z2"},
		expectedDriverNode: "n1_z1",
		willFit:            true,
		expectedCounts:     map[string]int{"n2_z1": 2},
	}, {
		name:              "picks the zone where application fits entirely",
		driverResources:   createResources(1, 3),
		executorResources: createResources(2, 5),
		numExecutors:      2,
		availableResources: resources.NodeGroupResources(map[string]*resources.Resources{
			"n1_z1": createResources(4, 5),
			"n1_z2": createResources(4, 8),
			"n2_z2": createResources(6, 20),
		}),
		nodeZoneLabels:     map[string]string{"n1_z1": "z1", "n1_z2": "z2", "n2_z2": "z2"},
		nodePriorityOrder:  []string{"n1_z1", "n1_z2", "n2_z2"},
		expectedDriverNode: "n1_z2",
		willFit:            true,
		expectedCounts:     map[string]int{"n1_z2": 1, "n2_z2": 1},
	}, {
		name:              "falls back to cross zone allocation if application does not fit entirely in one zone",
		driverResources:   createResources(1, 3),
		executorResources: createResources(2, 5),
		numExecutors:      2,
		availableResources: resources.NodeGroupResources(map[string]*resources.Resources{
			"n1_z1": createResources(4, 5),
			"n2_z1": createResources(4, 6),
			"n1_z2": createResources(4, 7),
			"n2_z2": createResources(6, 7),
		}),
		nodeZoneLabels:     map[string]string{"n1_z1": "z1", "n1_z2": "z2", "n2_z1": "z1", "n2_z2": "z2"},
		nodePriorityOrder:  []string{"n1_z1", "n2_z1", "n1_z2", "n2_z2"},
		expectedDriverNode: "n1_z1",
		willFit:            true,
		expectedCounts:     map[string]int{"n2_z1": 1, "n1_z2": 1},
	}, {
		name:              "allocation works if zone labels are not available",
		driverResources:   createResources(1, 3),
		executorResources: createResources(2, 5),
		numExecutors:      5,
		availableResources: resources.NodeGroupResources(map[string]*resources.Resources{
			"n1": createResources(11, 28),
			"n2": createResources(10, 20),
		}),
		nodeZoneLabels:     map[string]string{},
		nodePriorityOrder:  []string{"n1", "n2"},
		expectedDriverNode: "n1",
		willFit:            true,
		expectedCounts:     map[string]int{"n1": 5},
	}}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			driver, executors, ok := AzAwareTightlyPack(
				context.Background(),
				test.driverResources,
				test.executorResources,
				test.numExecutors,
				test.nodePriorityOrder,
				test.nodePriorityOrder,
				test.nodeZoneLabels,
				test.availableResources)
			if ok != test.willFit {
				t.Fatalf("mismatch in willFit, expected: %v, got: %v", test.willFit, ok)
			}
			if !test.willFit {
				return
			}
			if driver != test.expectedDriverNode {
				t.Fatalf("mismatch in driver node, expected: %v, got: %v", test.expectedDriverNode, driver)
			}
			resultCounts := map[string]int{}
			for _, node := range executors {
				resultCounts[node]++
			}
			if test.expectedCounts != nil && !reflect.DeepEqual(resultCounts, test.expectedCounts) {
				t.Fatalf("executor nodes are not equal, expected: %v, got: %v", test.expectedCounts, resultCounts)
			}
		})
	}
}
