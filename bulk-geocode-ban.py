import os
import math
import requests
import io
import time

import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa


ADDOK_URL = 'http://10.233.23.114:7878/search/csv/'


def geocode_bulk(filepath_in, n_rows_per_batch, requests_options):
    b = os.path.getsize(filepath_in)
    intermediate_files = []
    with open(filepath_in, 'r') as bigfile:
        row_count = sum(1 for row in bigfile)
    with open(filepath_in, 'r') as bigfile:
        headers = bigfile.readline()
        chunk_by = math.ceil(b / row_count * n_rows_per_batch)
        current_lines = bigfile.readlines(chunk_by)
        i = 1
        # import ipdb;ipdb.set_trace()
        while current_lines:
            # Send batch to addok as CSV
            current_filename = 'result-{}.csv'.format(i)
            current_csv = ''.join([headers] + current_lines)
            filename, response = post_to_addok(current_filename, current_csv, requests_options)
            # Export intermediate results to parquet
            current_filename_pq = current_filename.replace(".csv", ".parquet")
            df_intermediate = pd.read_csv(io.StringIO(response.content.decode("utf-8")))
            df_intermediate = df_intermediate[["id_ea", "idlogement", "depcom", "adresse",
                                               "result_id", "result_score", "result_label"]]
            df_intermediate.rename({"result_id": "ban_id",
                                    "result_score": "ban_score",
                                    "result_label": "ban_label"})
            df_intermediate.to_parquet(current_filename_pq)
            # Log & increment
            print(f"Batch {i} done ({i*n_rows_per_batch} / {row_count} rows).")
            current_lines = bigfile.readlines(chunk_by)
            i += 1
            intermediate_files.append(current_filename_pq)

    # Merge intermediate csv files into a single parquet file
    intermediate_tables = [pq.read_table(file) for file in intermediate_files]
    combined_table = pa.concat_tables(intermediate_tables)
    output_filename = filepath_in.replace(".csv", "") + ".geocoded" + ".parquet"
    pq.write_table(combined_table, output_filename)

    # Clean intermediate files
    for file in intermediate_files:
        if os.path.isfile(file):
            os.remove(file)


def post_to_addok(filename, filelike_object, requests_options):
    files = {'data': (filename, filelike_object)}
    response = requests.post(ADDOK_URL, files=files, data=requests_options)
    # You might want to use https://github.com/g2p/rfc6266
    content_disposition = response.headers['content-disposition']
    filename = content_disposition[len('attachment; filename="'):-len('"')]
    return filename, response


if __name__ == "__main__":
    start = time.time()
    geocode_bulk(filepath_in="data/adresses_ril_achille.csv",
                 n_rows_per_batch=2000,
                 requests_options={
                     "citycode": "depcom",
                     "columns": "adresse"
                     }
                 )
    end = time.time()
    print(f"Geocoding done in {end - start} seconds.")
