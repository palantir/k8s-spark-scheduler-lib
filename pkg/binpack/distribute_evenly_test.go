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
	"k8s.io/apimachinery/pkg/api/resource"
)

func createResources(cpu, memory int64) *resources.Resources {
	return &resources.Resources{
		CPU:    *resource.NewQuantity(cpu, resource.DecimalSI),
		Memory: *resource.NewQuantity(memory, resource.BinarySI),
	}
}

func TestDistributeEvenly(t *testing.T) {
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
		name:              "application fits",
		driverResources:   createResources(1, 3),
		executorResources: createResources(2, 5),
		numExecutors:      2,
		availableResources: resources.NodeGroupResources(map[string]*resources.Resources{
			"n1": createResources(5, 10),
			"n2": createResources(4, 5),
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
		availableResources: map[string]*resources.Resources{
			"n1": createResources(2, 3),
		},
		nodePriorityOrder: []string{"n1"},
		willFit:           false,
		expectedCounts:    nil,
	}, {
		name:              "application perfectly fits",
		driverResources:   createResources(1, 2),
		executorResources: createResources(1, 1),
		numExecutors:      40,
		availableResources: map[string]*resources.Resources{
			"n1": createResources(13, 14),
			"n2": createResources(12, 12),
			"n3": createResources(16, 16),
		},
		nodePriorityOrder:  []string{"n1", "n2", "n3"},
		expectedDriverNode: "n1",
		willFit:            true,
		expectedCounts:     map[string]int{"n1": 12, "n2": 12, "n3": 16},
	}, {
		name:              "executor cpu do not fit",
		driverResources:   createResources(1, 1),
		executorResources: createResources(1, 2),
		numExecutors:      8,
		availableResources: map[string]*resources.Resources{
			"n1": createResources(8, 20),
		},
		nodePriorityOrder: []string{"n1"},
		willFit:           false,
		expectedCounts:    nil,
	}, {
		name:              "Fits when cluster has more nodes than executors",
		driverResources:   createResources(1, 2),
		executorResources: createResources(2, 3),
		numExecutors:      2,
		availableResources: map[string]*resources.Resources{
			"n1": createResources(8, 20),
			"n2": createResources(8, 20),
			"n3": createResources(8, 20),
		},
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
