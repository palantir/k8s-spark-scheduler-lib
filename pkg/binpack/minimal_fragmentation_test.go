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

func TestMinimalFragmentation(t *testing.T) {
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
		name:              "application fits",
		driverResources:   resources.CreateResources(1, 3, 1),
		executorResources: resources.CreateResources(2, 5, 1),
		numExecutors:      2,
		nodesSchedulingMetadata: resources.NodeGroupSchedulingMetadata(map[string]*resources.NodeSchedulingMetadata{
			"n1": resources.CreateSchedulingMetadata(5, 10, 2, "zone1"),
			"n2": resources.CreateSchedulingMetadata(4, 5, 1, "zone1"),
		}),
		nodePriorityOrder:  []string{"n1", "n2"},
		expectedDriverNode: "n1",
		willFit:            true,
		expectedCounts:     map[string]int{"n1": 1, "n2": 1},
	}, {
		name:              "when not fitting on a single node, executors are first fitted on nodes with the most available resources",
		driverResources:   resources.CreateResources(1, 3, 0),
		executorResources: resources.CreateResources(2, 5, 0),
		numExecutors:      5,
		nodesSchedulingMetadata: resources.NodeGroupSchedulingMetadata(map[string]*resources.NodeSchedulingMetadata{
			"n1": resources.CreateSchedulingMetadata(5, 25, 6, "zone1"),
			"n2": resources.CreateSchedulingMetadata(9, 24, 6, "zone1"),
		}),
		nodePriorityOrder:  []string{"n1", "n2"},
		expectedDriverNode: "n1",
		willFit:            true,
		expectedCounts:     map[string]int{"n1": 1, "n2": 4},
	}, {
		name:              "successfully fits executor-less applications",
		driverResources:   resources.CreateResources(1, 3, 0),
		executorResources: resources.CreateResources(2, 5, 0),
		numExecutors:      0,
		nodesSchedulingMetadata: resources.NodeGroupSchedulingMetadata(map[string]*resources.NodeSchedulingMetadata{
			"n1": resources.CreateSchedulingMetadata(5, 25, 6, "zone1"),
			"n2": resources.CreateSchedulingMetadata(5, 25, 6, "zone1"),
		}),
		nodePriorityOrder:  []string{"n1", "n2"},
		expectedDriverNode: "n1",
		willFit:            true,
	}, {
		name:              "successfully fits executor-less applications, and accounts for existing reservations",
		driverResources:   resources.CreateResources(1, 3, 0),
		executorResources: resources.CreateResources(2, 5, 0),
		numExecutors:      1,
		nodesSchedulingMetadata: resources.NodeGroupSchedulingMetadata(map[string]*resources.NodeSchedulingMetadata{
			"n1": resources.CreateSchedulingMetadata(5, 25, 6, "zone1"),
			"n2": resources.CreateSchedulingMetadata(5, 25, 6, "zone1"),
		}),
		nodePriorityOrder:  []string{"n1", "n2"},
		expectedDriverNode: "n1",
		willFit:            true,
		expectedCounts:     map[string]int{"n1": 1},
	}, {
		name:              "executors fit on a single node, but we avoid empty nodes if possible",
		driverResources:   resources.CreateResources(1, 3, 0),
		executorResources: resources.CreateResources(2, 5, 0),
		numExecutors:      5,
		nodesSchedulingMetadata: resources.NodeGroupSchedulingMetadata(map[string]*resources.NodeSchedulingMetadata{
			"n1": resources.CreateSchedulingMetadata(10, 25, 6, "zone1"),
			"n2": resources.CreateSchedulingMetadata(5, 25, 6, "zone1"),
			"n3": resources.CreateSchedulingMetadata(100, 100, 6, "zone1"),
		}),
		nodePriorityOrder:  []string{"n1", "n2", "n3"},
		expectedDriverNode: "n1",
		willFit:            true,
		expectedCounts:     map[string]int{"n1": 4, "n2": 1},
	}, {
		name:              "executors fits perfectly on a single node, but we avoid empty nodes if possible",
		driverResources:   resources.CreateResources(1, 3, 0),
		executorResources: resources.CreateResources(2, 5, 0),
		numExecutors:      5,
		nodesSchedulingMetadata: resources.NodeGroupSchedulingMetadata(map[string]*resources.NodeSchedulingMetadata{
			"n1": resources.CreateSchedulingMetadata(10, 25, 6, "zone1"),
			"n2": resources.CreateSchedulingMetadata(5, 25, 6, "zone1"),
			"n3": resources.CreateSchedulingMetadata(20, 25, 6, "zone1"),
		}),
		nodePriorityOrder:  []string{"n1", "n2", "n3"},
		expectedDriverNode: "n1",
		willFit:            true,
		expectedCounts:     map[string]int{"n3": 5},
	}, {
		name:              "executors fit on the smallest nodes that can accommodate all of them",
		driverResources:   resources.CreateResources(1, 3, 0),
		executorResources: resources.CreateResources(2, 5, 0),
		numExecutors:      5,
		nodesSchedulingMetadata: resources.NodeGroupSchedulingMetadata(map[string]*resources.NodeSchedulingMetadata{
			"n1": resources.CreateSchedulingMetadata(200, 500, 6, "zone1"),
			"n2": resources.CreateSchedulingMetadata(100, 250, 6, "zone1"),
		}),
		nodePriorityOrder:  []string{"n1", "n2", "n3"},
		expectedDriverNode: "n1",
		willFit:            true,
		expectedCounts:     map[string]int{"n2": 5},
	}, {
		name:              "consider 'empty' nodes when the app can't fit on 'non-empty' ones",
		driverResources:   resources.CreateResources(1, 3, 0),
		executorResources: resources.CreateResources(2, 5, 0),
		numExecutors:      5,
		nodesSchedulingMetadata: resources.NodeGroupSchedulingMetadata(map[string]*resources.NodeSchedulingMetadata{
			"n1": resources.CreateSchedulingMetadata(1, 3, 6, "zone1"),
			"n2": resources.CreateSchedulingMetadata(8, 10, 6, "zone1"),
			"n3": resources.CreateSchedulingMetadata(20, 25, 6, "zone1"),
			"n4": resources.CreateSchedulingMetadata(20, 25, 6, "zone1"),
			"n5": resources.CreateSchedulingMetadata(20, 25, 6, "zone1"),
		}),
		nodePriorityOrder:  []string{"n1", "n2", "n3", "n4", "n5"},
		expectedDriverNode: "n1",
		willFit:            true,
		expectedCounts:     map[string]int{"n3": 5},
	}, {
		name:              "when available resources are equal, prefer nodes according to the requested priorities",
		driverResources:   resources.CreateResources(1, 3, 0),
		executorResources: resources.CreateResources(2, 5, 0),
		numExecutors:      5,
		nodesSchedulingMetadata: resources.NodeGroupSchedulingMetadata(map[string]*resources.NodeSchedulingMetadata{
			"n1": resources.CreateSchedulingMetadata(1, 3, 6, "zone1"),
			"n2": resources.CreateSchedulingMetadata(8, 10, 6, "zone1"),
			"n3": resources.CreateSchedulingMetadata(20, 25, 6, "zone1"),
			"n4": resources.CreateSchedulingMetadata(20, 25, 6, "zone1"),
			"n5": resources.CreateSchedulingMetadata(20, 25, 6, "zone1"),
		}),
		nodePriorityOrder:  []string{"n1", "n2", "n3", "n4", "n5"},
		expectedDriverNode: "n1",
		willFit:            true,
		expectedCounts:     map[string]int{"n3": 5},
	}, {
		name:              "picks the smallest node that fits the remaining executors",
		driverResources:   resources.CreateResources(1, 3, 0),
		executorResources: resources.CreateResources(2, 5, 0),
		numExecutors:      5,
		nodesSchedulingMetadata: resources.NodeGroupSchedulingMetadata(map[string]*resources.NodeSchedulingMetadata{
			"n1": resources.CreateSchedulingMetadata(6, 30, 6, "zone1"),
			"n2": resources.CreateSchedulingMetadata(3, 30, 6, "zone1"),
			"n3": resources.CreateSchedulingMetadata(8, 30, 6, "zone1"),
		}),
		nodePriorityOrder:  []string{"n1", "n2", "n3"},
		expectedDriverNode: "n1",
		willFit:            true,
		expectedCounts:     map[string]int{"n2": 1, "n3": 4},
	}, {
		name:              "driver memory does not fit",
		driverResources:   resources.CreateResources(2, 4, 1),
		executorResources: resources.CreateResources(1, 1, 0),
		numExecutors:      1,
		nodesSchedulingMetadata: resources.NodeGroupSchedulingMetadata(map[string]*resources.NodeSchedulingMetadata{
			"n1": resources.CreateSchedulingMetadata(2, 3, 1, "zone1"),
		}),
		nodePriorityOrder: []string{"n1"},
		willFit:           false,
	}, {
		name:              "application perfectly fits",
		driverResources:   resources.CreateResources(1, 2, 1),
		executorResources: resources.CreateResources(1, 1, 1),
		numExecutors:      40,
		nodesSchedulingMetadata: resources.NodeGroupSchedulingMetadata(map[string]*resources.NodeSchedulingMetadata{
			"n1": resources.CreateSchedulingMetadata(13, 14, 13, "zone1"),
			"n2": resources.CreateSchedulingMetadata(12, 12, 12, "zone1"),
			"n3": resources.CreateSchedulingMetadata(16, 16, 16, "zone1"),
		}),
		nodePriorityOrder:  []string{"n1", "n2", "n3"},
		expectedDriverNode: "n1",
		willFit:            true,
		expectedCounts:     map[string]int{"n1": 12, "n2": 12, "n3": 16},
	}, {
		name:              "executor cpu do not fit",
		driverResources:   resources.CreateResources(1, 1, 0),
		executorResources: resources.CreateResources(1, 2, 1),
		numExecutors:      8,
		nodesSchedulingMetadata: resources.NodeGroupSchedulingMetadata(map[string]*resources.NodeSchedulingMetadata{
			"n1": resources.CreateSchedulingMetadata(8, 20, 8, "zone1"),
		}),
		nodePriorityOrder: []string{"n1"},
		willFit:           false,
	}, {
		name:              "Fits when cluster has more nodes than executors",
		driverResources:   resources.CreateResources(1, 2, 1),
		executorResources: resources.CreateResources(2, 3, 2),
		numExecutors:      2,
		nodesSchedulingMetadata: resources.NodeGroupSchedulingMetadata(map[string]*resources.NodeSchedulingMetadata{
			"n1": resources.CreateSchedulingMetadata(8, 20, 8, "zone1"),
			"n2": resources.CreateSchedulingMetadata(8, 20, 8, "zone1"),
			"n3": resources.CreateSchedulingMetadata(8, 20, 8, "zone1"),
		}),
		nodePriorityOrder:  []string{"n1", "n2", "n3"},
		expectedDriverNode: "n1",
		willFit:            true,
	}, {
		name:              "executor gpu does not fit",
		driverResources:   resources.CreateResources(1, 1, 1),
		executorResources: resources.CreateResources(1, 1, 1),
		numExecutors:      4,
		nodesSchedulingMetadata: resources.NodeGroupSchedulingMetadata(map[string]*resources.NodeSchedulingMetadata{
			"n1_z1": resources.CreateSchedulingMetadata(4, 4, 4, "z1"),
			"n1_z2": resources.CreateSchedulingMetadata(128, 128, 0, "z2"),
		}),
		nodePriorityOrder:  []string{"n1_z1", "n1_z2"},
		expectedDriverNode: "n1_z1",
		willFit:            false,
	},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			packingResult := MinimalFragmentation(
				context.Background(),
				test.driverResources,
				test.executorResources,
				test.numExecutors,
				test.nodePriorityOrder,
				test.nodePriorityOrder,
				test.nodesSchedulingMetadata)
			if packingResult.HasCapacity != test.willFit {
				t.Fatalf("mismatch in willFit, expected: %v, got: %v", test.willFit, packingResult.HasCapacity)
			}
			if !test.willFit {
				return
			}
			if packingResult.DriverNode != test.expectedDriverNode {
				t.Fatalf("mismatch in driver node, expected: %v, got: %v", test.expectedDriverNode, packingResult.DriverNode)
			}
			resultCounts := map[string]int{}
			for _, node := range packingResult.ExecutorNodes {
				resultCounts[node]++
			}
			if test.expectedCounts != nil && !reflect.DeepEqual(resultCounts, test.expectedCounts) {
				t.Fatalf("executor nodes are not equal, expected: %v, got: %v", test.expectedCounts, resultCounts)
			}
		})
	}
}
