module github.com/palantir/k8s-spark-scheduler-lib

go 1.18

require (
	github.com/google/go-cmp v0.5.6
	github.com/palantir/witchcraft-go-error v1.4.3
	github.com/stretchr/testify v1.7.0
	k8s.io/api v0.18.8
	k8s.io/apiextensions-apiserver v0.18.8
	k8s.io/apimachinery v0.18.8
	k8s.io/client-go v0.18.8
	k8s.io/code-generator v0.18.8
	sigs.k8s.io/controller-runtime v0.6.4
)

require google.golang.org/appengine v1.6.1 // indirect
