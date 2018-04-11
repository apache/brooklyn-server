
# Kubernetes Location

Brooklyn Container Location has an extensive support for Kubernetes deployments
In particular, it supports

- KubernetesResource
- KubernetesHelmChart
- KubernetesContainer

## Kubernets Helm Chart

Here's an example of an Helm based blueprint 

```YAML
location:
  kubernetes:
    endpoint: https://localhost:6443
    kubeconfig: /home/user/.kube/config
services:
- type: org.apache.brooklyn.container.entity.kubernetes.KubernetesHelmChart
  name: jenkins-helm
  chartName: jenkins
```

Notice, in this case, it is pointing at a local k8s cluster (created using Docker on Mac) and specify a `kubeconfig` 
file for connection details.

The `KubernetesHelmChart` entity will install the latest version of the `chart` named `jenkins` from the Chart repository `stable` at `https://kubernetes-charts.storage.googleapis.com/`
You can install a specific version of the chart by using `chartVersion` config key.