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

package resources

import (
	"reflect"
	"testing"

	"k8s.io/apimachinery/pkg/api/resource"
)

func createResources(cpu, memory int64) *Resources {
	return &Resources{
		CPU:    *resource.NewQuantity(cpu, resource.DecimalSI),
		Memory: *resource.NewQuantity(memory, resource.BinarySI),
	}
}

func TestAdd(t *testing.T) {
	first := NodeGroupResources(map[string]*Resources{"1": createResources(1, 2), "2": createResources(3, 10)})
	second := NodeGroupResources(map[string]*Resources{"1": createResources(2, 4), "3": createResources(1, 5)})
	result := NodeGroupResources(map[string]*Resources{"1": createResources(3, 6), "2": createResources(3, 10), "3": createResources(1, 5)})
	first.Add(second)
	if !reflect.DeepEqual(first, result) {
		t.Fatalf("sum not equal, expected: %+v, got: %+v", result, first)
	}
}

func TestSub(t *testing.T) {
	first := NodeGroupResources(map[string]*Resources{"1": createResources(1, 2), "2": createResources(3, 10)})
	second := NodeGroupResources(map[string]*Resources{"1": createResources(2, 4), "3": createResources(1, 5)})
	result := NodeGroupResources(map[string]*Resources{"1": createResources(-1, -2), "2": createResources(3, 10), "3": createResources(-1, -5)})
	first.Sub(second)
	if !reflect.DeepEqual(first, result) {
		t.Fatalf("difference not equal, expected: %+v, got: %+v", result, first)
	}
}
