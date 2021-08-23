#!/usr/bin/env bash

set -o errexit
set -o nounset
set -o pipefail

SCRIPT_ROOT=$(dirname ${BASH_SOURCE})/..
CODEGEN_PKG=${CODEGEN_PKG:-$(cd ${SCRIPT_ROOT}; ls -d -1 ./vendor/k8s.io/code-generator 2>/dev/null || echo ../code-generator)}

bash ${CODEGEN_PKG}/generate-groups.sh "deepcopy,client,informer,lister" \
  github.com/palantir/k8s-spark-scheduler-lib/pkg/client \
  github.com/palantir/k8s-spark-scheduler-lib/pkg/apis \
  'sparkscheduler:v1beta1 sparkscheduler:v1beta2 scaler:v1alpha1 scaler:v1alpha2' \
  --go-header-file "${SCRIPT_ROOT}"/hack/boilerplate.go.txt

