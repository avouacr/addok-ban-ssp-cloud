# Deploy a addok/BAN instance on the SSP Cloud to geocode adresses

## Steps

- Retrieve [addok](https://addok.readthedocs.io/en/latest/)'s config and pre-indexed BAN (refer to [documentation](https://github.com/BaseAdresseNationale/addok-docker?tab=readme-ov-file#guides-dinstallation) for more info)

```bash
cd addok-ban-ssp-cloud
./get-addok-bundle.sh
```

- Deploy on `Kubernetes`

```bash
kubectl apply -f kubernetes/
```

- Adapt the `bulk-geocode-ban.py` to your use case
