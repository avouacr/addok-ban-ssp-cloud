#!/bin/bash

# See : https://github.com/BaseAdresseNationale/addok-docker?tab=readme-ov-file#guides-dinstallation

# Get addok bundle
wget https://adresse.data.gouv.fr/data/ban/adresses/latest/addok/addok-france-bundle.zip
unzip -d addok-bundle addok-france-bundle.zip

# Load on personal S3 bucket and enable public download
mc cp --recursive addok-bundle/ s3/avouacr/addok
mc anonymous set download s3/avouacr/addok/
