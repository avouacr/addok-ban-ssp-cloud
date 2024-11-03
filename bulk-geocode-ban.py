"""Bulk geocode addresses using the BAN.

Reference : https://guides.data.gouv.fr/reutiliser-des-donnees/utiliser-les-api-geographiques/utiliser-lapi-adresse/geocoder-des-adresses-pratique#geocodage-massif
"""
import os
import math
import shutil
import requests


ADDOK_URL = 'http://10.233.23.114:7878/search/csv/'


def geocode_chunked(filepath_in, filename_pattern, chunk_by_approximate_lines, requests_options):
    b = os.path.getsize(filepath_in)
    output_files = []
    with open(filepath_in, 'r') as bigfile:
        row_count = sum(1 for row in bigfile)
    with open(filepath_in, 'r') as bigfile:
        headers = bigfile.readline()
        chunk_by = math.ceil(b / row_count * chunk_by_approximate_lines)
        current_lines = bigfile.readlines(chunk_by)
        i = 1
        # import ipdb;ipdb.set_trace()
        while current_lines:
            current_filename = filename_pattern.format(i)
            current_csv = ''.join([headers] + current_lines)
            # import ipdb;ipdb.set_trace()
            filename, response = post_to_addok(current_filename, current_csv, requests_options)
            write_response_to_disk(current_filename, response)
            current_lines = bigfile.readlines(chunk_by)
            i += 1
            output_files.append(current_filename)
    return output_files


def write_response_to_disk(filename, response, chunk_size=1024):
    with open(filename, 'wb') as fd:
        for chunk in response.iter_content(chunk_size=chunk_size):
            fd.write(chunk)


def post_to_addok(filename, filelike_object, requests_options):
    files = {'data': (filename, filelike_object)}
    response = requests.post(ADDOK_URL, files=files, data=requests_options)
    # You might want to use https://github.com/g2p/rfc6266
    content_disposition = response.headers['content-disposition']
    filename = content_disposition[len('attachment; filename="'):-len('"')]
    return filename, response


def consolidate_multiple_csv(files, output_name):
    with open(output_name, 'wb') as outfile:
        for i, fname in enumerate(files):
            with open(fname, 'rb') as infile:
                if i != 0:
                    infile.readline()  # Throw away header on all but first file
                # Block copy rest of file from input to output without parsing
                shutil.copyfileobj(infile, outfile)


myfiles = geocode_chunked(filepath_in='adresses_ril_achille.csv',
                          filename_pattern='result-{}.csv',
                          chunk_by_approximate_lines=1000,
                          requests_options={"columns": ['ADRESSE', 'CODE POSTAL', 'COMMUNE']}
                          )
# Merge files
consolidate_multiple_csv(myfiles, 'adresses_ril_achille.geocoded.csv')

# Clean tmp files
[os.remove(f) for f in myfiles if os.path.isfile(f)]
