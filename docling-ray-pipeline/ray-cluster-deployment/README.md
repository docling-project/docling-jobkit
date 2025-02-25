## Deploying ray cluster using helm chart and operator

Provided `values.yaml` is made for `dev.accelerated-discovery.res.ibm.com` cluster in YKT.
The KubeRay Operator is already deployed and serves `deep-search` namespace.

```
helm install raycluster kuberay/ray-cluster --version 1.1.0 -f ./ray-cluster-deployment/values.yaml
```


## Deploying ray cluster using helm chart and operator

The helm chart relies on presence of the KubeRay Operator.

```
helm repo add kuberay https://ray-project.github.io/kuberay-helm/

helm install kuberay-operator kuberay/kuberay-operator --version 1.1.0

helm upgrade --install raycluster kuberay/ray-cluster --version 1.1.0 -f ./ray-cluster-deployment/values.yaml

```