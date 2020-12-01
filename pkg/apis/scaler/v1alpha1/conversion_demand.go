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

package v1alpha1

import (
	"github.com/palantir/k8s-spark-scheduler-lib/pkg/apis/scaler/v1alpha2"
	werror "github.com/palantir/witchcraft-go-error"
	v1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/conversion"
)

// ConvertTo converts from v1alpha1 to the storage version v1alpha2
func (d *Demand) ConvertTo(dstRaw conversion.Hub) error {
	dst := dstRaw.(*v1alpha2.Demand)

	// Status
	dst.Status.LastTransitionTime = d.Status.LastTransitionTime
	dst.Status.Phase = d.Status.Phase

	// Spec
	dst.Spec.InstanceGroup = d.Spec.InstanceGroup
	dst.Spec.IsLongLived = d.Spec.IsLongLived

	dstUnits := make([]v1alpha2.DemandUnit, 0, len(d.Spec.Units))
	for _, u := range d.Spec.Units {
		dstUnits = append(dstUnits, v1alpha2.DemandUnit{
			Resources: v1.ResourceList{
				v1.ResourceCPU:    u.CPU,
				v1.ResourceMemory: u.Memory,
			},
			Count: u.Count,
		})
	}
	dst.Spec.Units = dstUnits

	// ObjectMeta
	dst.ObjectMeta = d.ObjectMeta

	return nil
}

// ConvertFrom converts from storage version v1alpha2 to v1alpha1
func (d *Demand) ConvertFrom(srcRaw conversion.Hub) error {
	src := srcRaw.(*v1alpha2.Demand)

	// Status
	d.Status.LastTransitionTime = src.Status.LastTransitionTime
	d.Status.Phase = src.Status.Phase

	// Spec
	d.Spec.InstanceGroup = src.Spec.InstanceGroup
	d.Spec.IsLongLived = src.Spec.IsLongLived

	dstUnits := make([]DemandUnit, 0, len(src.Spec.Units))
	for _, u := range src.Spec.Units {
		demandUnit := DemandUnit{
			Count: u.Count,
		}
		for name, quantity := range u.Resources {
			switch name {
			case v1.ResourceCPU:
				demandUnit.CPU = quantity
			case v1.ResourceMemory:
				demandUnit.Memory = quantity
			default:
				return werror.Error("unsupported resource found during demand conversion from storage version to v1alpha1",
					werror.SafeParam("resourceName", name),
					werror.SafeParam("resourceQuantity", quantity))
			}
		}
		dstUnits = append(dstUnits, demandUnit)
	}
	d.Spec.Units = dstUnits

	// ObjectMeta
	d.ObjectMeta = src.ObjectMeta

	return nil
}