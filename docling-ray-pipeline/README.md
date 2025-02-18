## Deploy ray cluster using helm

Follow guideline in the Helm folder to deploy ray cluster.
`docling-ray-pipeline/ray-cluster-deployment/README.md`



## Running ray apps

Creat virtual environment and install dependencies
```sh
python3.11 -m venv venv
source ./venv/bin/activate

pip install --upgrade pip
pip install -r requirements.txt
```

Assimung that ray cluster is already configured and deployed:

1. Open an oc port-forward to the deployed ray cluster:
```
oc port-forward --address 0.0.0.0 service/raycluster-raycluster-docling-head-svc 8265:8265
```
2. Visit http://localhost:8265/#/jobs

3. Export env
```
export RAY_ADDRESS=http://localhost:8265
```

To run an example ray-app use provided bash script:

```sh
./submit-docling-basic.sh
```



