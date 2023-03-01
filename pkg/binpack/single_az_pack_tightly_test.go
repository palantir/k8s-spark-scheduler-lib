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

func TestSingleAZTightlyPack(t *testing.T) {
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
		driverResources:   createResources(1, 3, 1),
		executorResources: createResources(2, 5, 2),
		numExecutors:      2,
		nodesSchedulingMetadata: resources.NodeGroupSchedulingMetadata(map[string]*resources.NodeSchedulingMetadata{
			"n1_z1": createSchedulingMetadata(4, 5, 4, "z1"),
			"n1_z2": createSchedulingMetadata(4, 8, 4, "z2"),
			"n2_z1": createSchedulingMetadata(6, 15, 6, "z1"),
			"n2_z2": createSchedulingMetadata(6, 20, 6, "z2"),
		}),
		nodePriorityOrder:  []string{"n1_z1", "n1_z2", "n2_z1", "n2_z2"},
		expectedDriverNode: "n1_z1",
		willFit:            true,
		expectedCounts:     map[string]int{"n2_z1": 2},
	}, {
		name:              "picks the zone where application fits entirely",
		driverResources:   createResources(1, 3, 1),
		executorResources: createResources(2, 5, 1),
		numExecutors:      2,
		nodesSchedulingMetadata: resources.NodeGroupSchedulingMetadata(map[string]*resources.NodeSchedulingMetadata{
			"n1_z1": createSchedulingMetadata(4, 5, 1, "z1"),
			"n1_z2": createSchedulingMetadata(4, 8, 2, "z2"),
			"n2_z2": createSchedulingMetadata(6, 20, 10, "z2"),
		}),
		nodePriorityOrder:  []string{"n1_z1", "n1_z2", "n2_z2"},
		expectedDriverNode: "n1_z2",
		willFit:            true,
		expectedCounts:     map[string]int{"n1_z2": 1, "n2_z2": 1},
	}, {
		name:              "Does not schedule if application does not fit entirely in one zone",
		driverResources:   createResources(1, 1, 1),
		executorResources: createResources(2, 1, 1),
		numExecutors:      2,
		nodesSchedulingMetadata: resources.NodeGroupSchedulingMetadata(map[string]*resources.NodeSchedulingMetadata{
			"n1_z1": createSchedulingMetadata(4, 5, 1, "z1"),
			"n2_z1": createSchedulingMetadata(4, 6, 1, "z1"),
			"n1_z2": createSchedulingMetadata(4, 7, 1, "z2"),
			"n2_z2": createSchedulingMetadata(6, 7, 0, "z2"),
		}),
		nodePriorityOrder:  []string{"n1_z1", "n2_z1", "n1_z2", "n2_z2"},
		expectedDriverNode: "n1_z1",
		willFit:            false,
	}, {
		name:              "executor gpu does not fit",
		driverResources:   createResources(1, 1, 1),
		executorResources: createResources(1, 1, 1),
		numExecutors:      4,
		nodesSchedulingMetadata: resources.NodeGroupSchedulingMetadata(map[string]*resources.NodeSchedulingMetadata{
			"n1_z1": createSchedulingMetadata(4, 4, 4, "z1"),
			"n1_z2": createSchedulingMetadata(128, 128, 0, "z2"),
		}),
		nodePriorityOrder:  []string{"n1_z1", "n1_z2"},
		expectedDriverNode: "n1_z1",
		willFit:            false,
	},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			p := SingleAZTightlyPack(
				context.Background(),
				test.driverResources,
				test.executorResources,
				test.numExecutors,
				test.nodePriorityOrder,
				test.nodePriorityOrder,
				test.nodesSchedulingMetadata)
			driver, executors, ok := p.driverNode, p.executorNodes, p.hasCapacity
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
