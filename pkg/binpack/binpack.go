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

	"github.com/palantir/k8s-spark-scheduler-lib/pkg/resources"
)

// PackingResult is a result of one binpacking operation. When successful, assigns driver and
// executors to nodes. Includes an overview of the resource assignment across nodes.
type PackingResult struct {
	DriverNode           string
	ExecutorNodes        []string
	AvgPackingEfficiency PackingEfficiency
	PackingEfficiencies  []*PackingEfficiency
	HasCapacity          bool
}

func emptyPackingResult() *PackingResult {
	return &PackingResult{
		DriverNode:    "",
		ExecutorNodes: nil,
		HasCapacity:   false,
	}
}

// SparkBinPackFunction is a function type for assigning nodes to spark drivers and executors
type SparkBinPackFunction func(
	ctx context.Context,
	driverResources, executorResources *resources.Resources,
	executorCount int,
	driverNodePriorityOrder, executorNodePriorityOrder []string,
	nodesSchedulingMetadata resources.NodeGroupSchedulingMetadata) *PackingResult

// GenericBinPackFunction is a function type for assigning nodes to a batch of equivalent pods
type GenericBinPackFunction func(
	ctx context.Context,
	itemResources *resources.Resources,
	itemCount int,
	nodePriorityOrder []string,
	nodesSchedulingMetadata resources.NodeGroupSchedulingMetadata,
	reservedResources resources.NodeGroupResources) (nodes []string, hasCapacity bool)

// SparkBinPack places the driver first and calls distributeExecutors function to place executors
func SparkBinPack(
	ctx context.Context,
	driverResources, executorResources *resources.Resources,
	executorCount int,
	driverNodePriorityOrder, executorNodePriorityOrder []string,
	nodesSchedulingMetadata resources.NodeGroupSchedulingMetadata,
	distributeExecutors GenericBinPackFunction) *PackingResult {
	for _, driverNode := range driverNodePriorityOrder {
		nodeSchedulingMetadata, ok := nodesSchedulingMetadata[driverNode]
		if !ok || driverResources.GreaterThan(nodeSchedulingMetadata.AvailableResources) {
			continue
		}
		reserved := make(resources.NodeGroupResources, len(nodesSchedulingMetadata))
		reserved[driverNode] = driverResources.Copy()
		executorNodes, ok := distributeExecutors(
			ctx, executorResources, executorCount, executorNodePriorityOrder, nodesSchedulingMetadata, reserved)
		if ok {
			avgPackingEfficiency, packingEfficiencies := ComputePackingEfficiencies(nodesSchedulingMetadata, reserved)
			return &PackingResult{
				DriverNode:           driverNode,
				ExecutorNodes:        executorNodes,
				HasCapacity:          true,
				AvgPackingEfficiency: avgPackingEfficiency,
				PackingEfficiencies:  packingEfficiencies,
			}
		}
	}
	return emptyPackingResult()
}
