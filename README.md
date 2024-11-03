# Deploy a addok/BAN instance on the SSP Cloud to geocode adresses

## Steps

- Retrieve [addok](https://github.com/BaseAdresseNationale/addok-docker?tab=readme-ov-file#guides-dinstallation)'s config and pre-indexed BAN (only useful if you want the most up to date version, else skip to next step)

```bash
cd addok-ban-ssp-cloud
./get-addok-bundle.sh
```

- Deploy on `Kubernetes`

```bash
kubectl apply -f kubernetes/
```

- Adapt the `bulk-geocode-ban.py` to your use case
