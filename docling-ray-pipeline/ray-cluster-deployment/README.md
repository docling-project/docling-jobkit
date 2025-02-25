## Deploying ray cluster using helm chart and operator

The helm chart relies on presence of the KubeRay Operator:
https://docs.ray.io/en/latest/cluster/kubernetes/index.html


Deploy Ray Operator and instance of the ray cluster, using helm charts and provided example of values.yaml:
```
helm repo add kuberay https://ray-project.github.io/kuberay-helm/

helm install kuberay-operator kuberay/kuberay-operator --version 1.1.0

helm upgrade --install raycluster kuberay/ray-cluster --version 1.1.0 -f ./ray-cluster-deployment/values.yaml

```