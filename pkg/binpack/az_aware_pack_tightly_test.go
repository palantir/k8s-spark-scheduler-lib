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

package binpack

import (
	"context"
	"reflect"
	"testing"

	"github.com/palantir/k8s-spark-scheduler-lib/pkg/resources"
)

func TestAzAwareTightlyPack(t *testing.T) {
	tests := []struct {
		name                    string
		driverResources         *resources.Resources
		executorResources       *resources.Resources
		numExecutors            int
		nodesSchedulingMetadata resources.NodeGroupSchedulingMetadata
		nodePriorityOrder       []string
		expectedDriverNode      string
		willFit                 bool
		expectedCounts          map[string]int
	}{{
		name:              "picks the first zone when application fits entirely in either of the zones",
		driverResources:   createResources(1, 3),
		executorResources: createResources(2, 5),
		numExecutors:      2,
		nodesSchedulingMetadata: resources.NodeGroupSchedulingMetadata(map[string]*resources.NodeSchedulingMetadata{
			"n1_z1": createSchedulingMetadata(4, 5, "z1"),
			"n1_z2": createSchedulingMetadata(4, 8, "z2"),
			"n2_z1": createSchedulingMetadata(6, 15, "z1"),
			"n2_z2": createSchedulingMetadata(6, 20, "z2"),
		}),
		nodePriorityOrder:  []string{"n1_z1", "n1_z2", "n2_z1", "n2_z2"},
		expectedDriverNode: "n1_z1",
		willFit:            true,
		expectedCounts:     map[string]int{"n2_z1": 2},
	}, {
		name:              "picks the zone where application fits entirely",
		driverResources:   createResources(1, 3),
		executorResources: createResources(2, 5),
		numExecutors:      2,
		nodesSchedulingMetadata: resources.NodeGroupSchedulingMetadata(map[string]*resources.NodeSchedulingMetadata{
			"n1_z1": createSchedulingMetadata(4, 5, "z1"),
			"n1_z2": createSchedulingMetadata(4, 8, "z2"),
			"n2_z2": createSchedulingMetadata(6, 20, "z2"),
		}),
		nodePriorityOrder:  []string{"n1_z1", "n1_z2", "n2_z2"},
		expectedDriverNode: "n1_z2",
		willFit:            true,
		expectedCounts:     map[string]int{"n1_z2": 1, "n2_z2": 1},
	}, {
		name:              "falls back to cross zone allocation if application does not fit entirely in one zone",
		driverResources:   createResources(1, 3),
		executorResources: createResources(2, 5),
		numExecutors:      2,
		nodesSchedulingMetadata: resources.NodeGroupSchedulingMetadata(map[string]*resources.NodeSchedulingMetadata{
			"n1_z1": createSchedulingMetadata(4, 5, "z1"),
			"n2_z1": createSchedulingMetadata(4, 6, "z1"),
			"n1_z2": createSchedulingMetadata(4, 7, "z2"),
			"n2_z2": createSchedulingMetadata(6, 7, "z2"),
		}),
		nodePriorityOrder:  []string{"n1_z1", "n2_z1", "n1_z2", "n2_z2"},
		expectedDriverNode: "n1_z1",
		willFit:            true,
		expectedCounts:     map[string]int{"n2_z1": 1, "n1_z2": 1},
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
				test.nodesSchedulingMetadata)
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
