package utils

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

func ConvertToGlobalObj(obj metav1.Object, clusterName string) {
	name := obj.GetName()
	namespace := obj.GetNamespace()
	if namespace == "" {
		obj.SetName(clusterName + "." + name)
	} else {
		obj.SetName(namespace + "." + name)
		obj.SetNamespace(clusterName)
	}
}
