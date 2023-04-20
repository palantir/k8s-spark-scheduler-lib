# Archived

This project is no longer maintained.

# Kubernetes Spark Scheduler Lib

[![CircleCI](https://circleci.com/gh/palantir/k8s-spark-scheduler-lib.svg?style=svg)](https://circleci.com/gh/palantir/k8s-spark-scheduler-lib)

`k8s-spark-scheduler-lib` contains the custom resource definitions and binpacking algorithms used by [k8s-spark-scheduler](https://github.com/palantir/k8s-spark-scheduler). This repo uses [godel](https://github.com/palantir/godel) as its build tool.

## Development

Use `./godelw verify` to run tests and style checks
Use `./hack/update-codegen.sh` to regenerate CRD clients, listers and informers.

# Contributing

The team welcomes contributions!  To make changes:

- Fork the repo and make a branch
- Write your code (ideally with tests) and make sure the CircleCI build passes
- Open a PR (optionally linking to a github issue)

# License
This project is made available under the [Apache 2.0 License](/LICENSE).
