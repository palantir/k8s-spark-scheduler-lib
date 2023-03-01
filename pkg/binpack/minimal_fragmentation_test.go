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
	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/api/resource"
)

func TestMin(t *testing.T) {
	assert.Equal(t, 1, min(1, 2, 3))
	assert.Equal(t, 1, min(2, 1, 3))
	assert.Equal(t, 1, min(2, 3, 1))
}

func TestGetNodeCapacity(t *testing.T) {
	singleExecutor := &resources.Resources{
		CPU:       *resource.NewQuantity(1, resource.DecimalSI),
		Memory:    *resource.NewQuantity(1, resource.DecimalSI),
		NvidiaGPU: *resource.NewQuantity(1, resource.DecimalSI),
	}

	tests := []struct {
		name           string
		available      *resources.Resources
		reserved       *resources.Resources
		singleExecutor *resources.Resources
		expected       int
	}{{
		name:           "no available resources",
		available:      &resources.Resources{},
		reserved:       resources.Zero(),
		singleExecutor: singleExecutor,
		expected:       0,
	}, {
		name:           "available resources fit exactly",
		available:      singleExecutor,
		reserved:       resources.Zero(),
		singleExecutor: singleExecutor,
		expected:       1,
	}, {
		name: "capacity is limited by cpu",
		available: &resources.Resources{
			CPU:       *resource.NewQuantity(3, resource.DecimalSI),
			Memory:    *resource.NewQuantity(4, resource.DecimalSI),
			NvidiaGPU: *resource.NewQuantity(4, resource.DecimalSI),
		},
		reserved:       resources.Zero(),
		singleExecutor: singleExecutor,
		expected:       3,
	}, {
		name: "capacity is limited by memory",
		available: &resources.Resources{
			CPU:       *resource.NewQuantity(4, resource.DecimalSI),
			Memory:    *resource.NewQuantity(3, resource.DecimalSI),
			NvidiaGPU: *resource.NewQuantity(4, resource.DecimalSI),
		},
		reserved:       resources.Zero(),
		singleExecutor: singleExecutor,
		expected:       3,
	}, {
		name: "capacity is limited by gpu",
		available: &resources.Resources{
			CPU:       *resource.NewQuantity(4, resource.DecimalSI),
			Memory:    *resource.NewQuantity(4, resource.DecimalSI),
			NvidiaGPU: *resource.NewQuantity(3, resource.DecimalSI),
		},
		reserved:       resources.Zero(),
		singleExecutor: singleExecutor,
		expected:       3,
	}, {
		name:      "does not fit due to existing reserved resources",
		available: singleExecutor,
		reserved: &resources.Resources{
			CPU: *resource.NewQuantity(1, resource.DecimalSI),
		},
		singleExecutor: singleExecutor,
		expected:       0,
	},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.expected, getNodeCapacity(test.available, test.reserved, test.singleExecutor))
		})
	}
}

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
		driverResources:   createResources(1, 3, 1),
		executorResources: createResources(2, 5, 1),
		numExecutors:      2,
		nodesSchedulingMetadata: resources.NodeGroupSchedulingMetadata(map[string]*resources.NodeSchedulingMetadata{
			"n1": createSchedulingMetadata(5, 10, 2, "zone1"),
			"n2": createSchedulingMetadata(4, 5, 1, "zone1"),
		}),
		nodePriorityOrder:  []string{"n1", "n2"},
		expectedDriverNode: "n1",
		willFit:            true,
		expectedCounts:     map[string]int{"n1": 1, "n2": 1},
	}, {
		name:              "when not fitting on a single node, executors are first fitted on nodes with the most available resources",
		driverResources:   createResources(1, 3, 0),
		executorResources: createResources(2, 5, 0),
		numExecutors:      5,
		nodesSchedulingMetadata: resources.NodeGroupSchedulingMetadata(map[string]*resources.NodeSchedulingMetadata{
			"n1": createSchedulingMetadata(5, 25, 6, "zone1"),
			"n2": createSchedulingMetadata(9, 24, 6, "zone1"),
		}),
		nodePriorityOrder:  []string{"n1", "n2"},
		expectedDriverNode: "n1",
		willFit:            true,
		expectedCounts:     map[string]int{"n1": 1, "n2": 4},
	}, {
		name:              "successfully fits executor-less applications",
		driverResources:   createResources(1, 3, 0),
		executorResources: createResources(2, 5, 0),
		numExecutors:      0,
		nodesSchedulingMetadata: resources.NodeGroupSchedulingMetadata(map[string]*resources.NodeSchedulingMetadata{
			"n1": createSchedulingMetadata(5, 25, 6, "zone1"),
			"n2": createSchedulingMetadata(5, 25, 6, "zone1"),
		}),
		nodePriorityOrder:  []string{"n1", "n2"},
		expectedDriverNode: "n1",
		willFit:            true,
	}, {
		name:              "successfully fits executor-less applications, and accounts for existing reservations",
		driverResources:   createResources(1, 3, 0),
		executorResources: createResources(2, 5, 0),
		numExecutors:      1,
		nodesSchedulingMetadata: resources.NodeGroupSchedulingMetadata(map[string]*resources.NodeSchedulingMetadata{
			"n1": createSchedulingMetadata(5, 25, 6, "zone1"),
			"n2": createSchedulingMetadata(5, 25, 6, "zone1"),
		}),
		nodePriorityOrder:  []string{"n1", "n2"},
		expectedDriverNode: "n1",
		willFit:            true,
		expectedCounts:     map[string]int{"n1": 1},
	}, {
		name:              "executors fit on a single node",
		driverResources:   createResources(1, 3, 0),
		executorResources: createResources(2, 5, 0),
		numExecutors:      5,
		nodesSchedulingMetadata: resources.NodeGroupSchedulingMetadata(map[string]*resources.NodeSchedulingMetadata{
			"n1": createSchedulingMetadata(10, 25, 6, "zone1"),
			"n2": createSchedulingMetadata(5, 25, 6, "zone1"),
			"n3": createSchedulingMetadata(20, 25, 6, "zone1"),
		}),
		nodePriorityOrder:  []string{"n1", "n2", "n3"},
		expectedDriverNode: "n1",
		willFit:            true,
		expectedCounts:     map[string]int{"n3": 5},
	}, {
		name:              "executors fit on the smallest nodes that can accommodate all of them",
		driverResources:   createResources(1, 3, 0),
		executorResources: createResources(2, 5, 0),
		numExecutors:      5,
		nodesSchedulingMetadata: resources.NodeGroupSchedulingMetadata(map[string]*resources.NodeSchedulingMetadata{
			"n1": createSchedulingMetadata(200, 500, 6, "zone1"),
			"n2": createSchedulingMetadata(100, 250, 6, "zone1"),
		}),
		nodePriorityOrder:  []string{"n1", "n2", "n3"},
		expectedDriverNode: "n1",
		willFit:            true,
		expectedCounts:     map[string]int{"n2": 5},
	}, {
		name:              "when available resources are equal, prefer nodes according to the requested priorities",
		driverResources:   createResources(1, 3, 0),
		executorResources: createResources(2, 5, 0),
		numExecutors:      5,
		nodesSchedulingMetadata: resources.NodeGroupSchedulingMetadata(map[string]*resources.NodeSchedulingMetadata{
			"n1": createSchedulingMetadata(10, 25, 6, "zone1"),
			"n2": createSchedulingMetadata(5, 25, 6, "zone1"),
			"n3": createSchedulingMetadata(20, 25, 6, "zone1"),
			"n4": createSchedulingMetadata(20, 25, 6, "zone1"),
			"n5": createSchedulingMetadata(20, 25, 6, "zone1"),
		}),
		nodePriorityOrder:  []string{"n1", "n2", "n3", "n4", "n5"},
		expectedDriverNode: "n1",
		willFit:            true,
		expectedCounts:     map[string]int{"n3": 5},
	}, {
		name:              "picks the smallest node that fits the remaining executors",
		driverResources:   createResources(1, 3, 0),
		executorResources: createResources(2, 5, 0),
		numExecutors:      5,
		nodesSchedulingMetadata: resources.NodeGroupSchedulingMetadata(map[string]*resources.NodeSchedulingMetadata{
			"n1": createSchedulingMetadata(6, 30, 6, "zone1"),
			"n2": createSchedulingMetadata(3, 30, 6, "zone1"),
			"n3": createSchedulingMetadata(8, 30, 6, "zone1"),
		}),
		nodePriorityOrder:  []string{"n1", "n2", "n3"},
		expectedDriverNode: "n1",
		willFit:            true,
		expectedCounts:     map[string]int{"n2": 1, "n3": 4},
	}, {
		name:              "driver memory does not fit",
		driverResources:   createResources(2, 4, 1),
		executorResources: createResources(1, 1, 0),
		numExecutors:      1,
		nodesSchedulingMetadata: resources.NodeGroupSchedulingMetadata(map[string]*resources.NodeSchedulingMetadata{
			"n1": createSchedulingMetadata(2, 3, 1, "zone1"),
		}),
		nodePriorityOrder: []string{"n1"},
		willFit:           false,
	}, {
		name:              "application perfectly fits",
		driverResources:   createResources(1, 2, 1),
		executorResources: createResources(1, 1, 1),
		numExecutors:      40,
		nodesSchedulingMetadata: resources.NodeGroupSchedulingMetadata(map[string]*resources.NodeSchedulingMetadata{
			"n1": createSchedulingMetadata(13, 14, 13, "zone1"),
			"n2": createSchedulingMetadata(12, 12, 12, "zone1"),
			"n3": createSchedulingMetadata(16, 16, 16, "zone1"),
		}),
		nodePriorityOrder:  []string{"n1", "n2", "n3"},
		expectedDriverNode: "n1",
		willFit:            true,
		expectedCounts:     map[string]int{"n1": 12, "n2": 12, "n3": 16},
	}, {
		name:              "executor cpu do not fit",
		driverResources:   createResources(1, 1, 0),
		executorResources: createResources(1, 2, 1),
		numExecutors:      8,
		nodesSchedulingMetadata: resources.NodeGroupSchedulingMetadata(map[string]*resources.NodeSchedulingMetadata{
			"n1": createSchedulingMetadata(8, 20, 8, "zone1"),
		}),
		nodePriorityOrder: []string{"n1"},
		willFit:           false,
	}, {
		name:              "Fits when cluster has more nodes than executors",
		driverResources:   createResources(1, 2, 1),
		executorResources: createResources(2, 3, 2),
		numExecutors:      2,
		nodesSchedulingMetadata: resources.NodeGroupSchedulingMetadata(map[string]*resources.NodeSchedulingMetadata{
			"n1": createSchedulingMetadata(8, 20, 8, "zone1"),
			"n2": createSchedulingMetadata(8, 20, 8, "zone1"),
			"n3": createSchedulingMetadata(8, 20, 8, "zone1"),
		}),
		nodePriorityOrder:  []string{"n1", "n2", "n3"},
		expectedDriverNode: "n1",
		willFit:            true,
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
			driver, executors, ok := MinimalFragmentation(
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
