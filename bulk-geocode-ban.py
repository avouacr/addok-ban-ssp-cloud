import requests
import io
import time

import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
from pyarrow.csv import write_csv

ADDOK_URL = 'http://10.233.23.114:7878/search/csv/'


def geocode_bulk(filepath_in, n_rows_per_batch, requests_options):
    # Open the Parquet file for reading
    parquet_file = pq.ParquetFile(filepath_in)
    row_count = parquet_file.metadata.num_rows

    # Use iter_batches to process the file in chunks
    geocoded_tables = []
    i = 1
    for batch in parquet_file.iter_batches(batch_size=n_rows_per_batch):
        # Convert pyarrow.Table to CSV directly
        csv_buffer = io.BytesIO()
        write_csv(batch, csv_buffer)
        csv_buffer.seek(0)

        # Send batch to addok as CSV
        _, response = post_to_addok(filename=f'result-{i}.csv',
                                    filelike_object=csv_buffer,
                                    requests_options=requests_options)

        # Process the API response directly into a pyarrow Table to avoid intermediate CSVs
        response_buffer = io.StringIO(response.content.decode("utf-8"))
        df_intermediate = pd.read_csv(response_buffer)

        # Rename columns and select relevant ones
        df_intermediate = df_intermediate.rename(columns={
            "result_id": "ban_id",
            "result_score": "ban_score",
            "result_label": "ban_label"
        })[["id_ea", "idlogement", "depcom", "adresse", "ban_id", "ban_score", "ban_label"]]

        # Convert the DataFrame to a pyarrow Table and store it in memory
        geocoded_tables.append(pa.Table.from_pandas(df_intermediate))

        # Log progress
        n_rows_done = min(i * n_rows_per_batch, row_count)
        print(f"Batch {i} done ({n_rows_done} / {row_count} rows).")

        i += 1

    # Concatenate all tables and write the final output once
    combined_table = pa.concat_tables(geocoded_tables)
    output_filename = filepath_in.replace(".parquet", "") + ".geocoded.parquet"
    pq.write_table(combined_table, output_filename)

    print(f"Geocoded data saved to {output_filename}")


def post_to_addok(filename, filelike_object, requests_options):
    files = {'data': (filename, filelike_object)}
    response = requests.post(ADDOK_URL, files=files, data=requests_options, stream=True)

    # Extract filename from headers
    content_disposition = response.headers.get('content-disposition', '')
    filename = content_disposition[len('attachment; filename="'):-len('"')]

    return filename, response


if __name__ == "__main__":
    start = time.time()
    geocode_bulk(filepath_in="data/sample.parquet",
                 n_rows_per_batch=2000,
                 requests_options={
                     "citycode": "depcom",
                     "columns": "adresse"
                 })
    end = time.time()
    print(f"Geocoding done in {end - start} seconds.")
