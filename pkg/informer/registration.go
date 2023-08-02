package informer

import "k8s.io/apimachinery/pkg/runtime"

type RuntimeObjectFunc func() runtime.Object

var instanceRegistration map[string]RuntimeObjectFunc = map[string]RuntimeObjectFunc{}

func RegisterObjectFunc(gvr string, initObjFunc RuntimeObjectFunc) {
	instanceRegistration[gvr] = initObjFunc
}

func GetObject(gvr string) runtime.Object {
	if initFunc, ok := instanceRegistration[gvr]; ok {
		return initFunc()
	}
	return nil
}
