package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
)

func main() {
	// Initialize the Kubernetes client
	kubeconfig := filepath.Join(homedir.HomeDir(), ".kube", "config")
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		fmt.Println("Error building kubeconfig:", err.Error())
		os.Exit(1)
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		fmt.Println("Error creating Kubernetes client:", err.Error())
		os.Exit(1)
	}

	// Fetch the GVR for Deployments
	deploy := &appsv1.Deployment{}

	// pretty.Println(deploy)
	v, _ := json.MarshalIndent(deploy, "", " ")
	fmt.Println(string(v))

	fmt.Println("Deployment Kind:", deploy.TypeMeta.Kind, "APIVersion:", deploy.TypeMeta.APIVersion)
	fmt.Println("Deployment GroupVersion:", deploy.GetResourceVersion())
	gvr, err := getDeploymentGVR(clientset, deploy.Kind)
	if err != nil {
		fmt.Println("Error getting Deployment GVR:", err.Error())
		os.Exit(1)
	}

	fmt.Println("Deployment GVR:", gvr)
}

func getDeploymentGVR(clientset *kubernetes.Clientset, kind string) (string, error) {
	// Get the API discovery client
	discoveryClient := clientset.Discovery()

	// Fetch the GVR for Deployments
	gv, err := discoveryClient.ServerPreferredResources()
	if err != nil {
		return "", fmt.Errorf("error getting API resource lists: %v", err)
	}

	for _, apiGroup := range gv {
		for _, apiResource := range apiGroup.APIResources {
			if apiResource.Kind == "Deployment" {
				return apiGroup.GroupVersion + "/" + apiResource.Name, nil
			}
		}
	}

	return "", fmt.Errorf("Deployment GVR not found")
}
