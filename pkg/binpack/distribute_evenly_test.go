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
	"k8s.io/apimachinery/pkg/api/resource"
)

func createResources(cpu, memory int64) *resources.Resources {
	return &resources.Resources{
		CPU:    *resource.NewQuantity(cpu, resource.DecimalSI),
		Memory: *resource.NewQuantity(memory, resource.BinarySI),
	}
}
func createSchedulingMetadata(cpu, memory int64, zoneLabel string) *resources.NodeSchedulingMetadata {
	return &resources.NodeSchedulingMetadata{
		AvailableResources: createResources(cpu, memory),
		ZoneLabel:          zoneLabel,
	}
}

func TestDistributeEvenly(t *testing.T) {
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
		driverResources:   createResources(1, 3),
		executorResources: createResources(2, 5),
		numExecutors:      2,
		nodesSchedulingMetadata: resources.NodeGroupSchedulingMetadata(map[string]*resources.NodeSchedulingMetadata{
			"n1": createSchedulingMetadata(5, 10, "zone1"),
			"n2": createSchedulingMetadata(4, 5, "zone1"),
		}),
		nodePriorityOrder:  []string{"n1", "n2"},
		expectedDriverNode: "n1",
		willFit:            true,
		expectedCounts:     map[string]int{"n1": 1, "n2": 1},
	}, {
		name:              "driver memory does not fit",
		driverResources:   createResources(2, 4),
		executorResources: createResources(1, 1),
		numExecutors:      1,
		nodesSchedulingMetadata: resources.NodeGroupSchedulingMetadata(map[string]*resources.NodeSchedulingMetadata{
			"n1": createSchedulingMetadata(2, 3, "zone1"),
		}),
		nodePriorityOrder: []string{"n1"},
		willFit:           false,
		expectedCounts:    nil,
	}, {
		name:              "application perfectly fits",
		driverResources:   createResources(1, 2),
		executorResources: createResources(1, 1),
		numExecutors:      40,
		nodesSchedulingMetadata: resources.NodeGroupSchedulingMetadata(map[string]*resources.NodeSchedulingMetadata{
			"n1": createSchedulingMetadata(13, 14, "zone1"),
			"n2": createSchedulingMetadata(12, 12, "zone1"),
			"n3": createSchedulingMetadata(16, 16, "zone1"),
		}),
		nodePriorityOrder:  []string{"n1", "n2", "n3"},
		expectedDriverNode: "n1",
		willFit:            true,
		expectedCounts:     map[string]int{"n1": 12, "n2": 12, "n3": 16},
	}, {
		name:              "executor cpu do not fit",
		driverResources:   createResources(1, 1),
		executorResources: createResources(1, 2),
		numExecutors:      8,
		nodesSchedulingMetadata: resources.NodeGroupSchedulingMetadata(map[string]*resources.NodeSchedulingMetadata{
			"n1": {
				AvailableResources: createResources(8, 20),
			},
		}),
		nodePriorityOrder: []string{"n1"},
		willFit:           false,
		expectedCounts:    nil,
	}, {
		name:              "Fits when cluster has more nodes than executors",
		driverResources:   createResources(1, 2),
		executorResources: createResources(2, 3),
		numExecutors:      2,
		nodesSchedulingMetadata: resources.NodeGroupSchedulingMetadata(map[string]*resources.NodeSchedulingMetadata{
			"n1": createSchedulingMetadata(8, 20, "zone1"),
			"n2": createSchedulingMetadata(8, 20, "zone1"),
			"n3": createSchedulingMetadata(8, 20, "zone1"),
		}),
		nodePriorityOrder:  []string{"n1", "n2", "n3"},
		expectedDriverNode: "n1",
		willFit:            true,
		expectedCounts:     nil,
	}}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			driver, executors, ok := DistributeEvenly(
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
