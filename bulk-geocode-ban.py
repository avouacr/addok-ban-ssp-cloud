import requests
import io
import time
import math
import logging

import pandas as pd
import pyarrow.parquet as pq
import pyarrow.dataset as ds
import pyarrow.compute as pc
import pyarrow as pa
from pyarrow.csv import write_csv


logging.basicConfig(
    level=logging.INFO,
    format="{asctime} - {levelname} - {message}",
    style="{",
    datefmt="%Y-%m-%d %H:%M"
    )


def geocode_bulk(filepath_in, requests_options, n_rows_per_batch=10000, sleeptime_between_batches=1):
    # Open the Parquet file for reading
    parquet_file = pq.ParquetFile(filepath_in)
    row_count = int(parquet_file.metadata.num_rows)
    if n_rows_per_batch is None:
        n_rows_per_batch = row_count

    n_batches = math.ceil(row_count / n_rows_per_batch)
    logging.info(f"Will geocode {row_count} rows as {n_batches} batches of size {n_rows_per_batch}.")

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
        df_intermediate = pd.read_csv(response_buffer, dtype={"depcom": "string"})

        # Convert the DataFrame to a pyarrow Table and store it in memory
        geocoded_tables.append(pa.Table.from_pandas(df_intermediate))

        # Log progress
        n_rows_done = min(i * n_rows_per_batch, row_count)
        logging.info(f"Batch {i} done ({n_rows_done} / {row_count} rows).")

        i += 1
        time.sleep(sleeptime_between_batches)

    # Concatenate all tables and write the final output in parquet, partitioned by dep
    combined_table = pa.concat_tables(geocoded_tables)
    dep = pc.if_else(
        pc.starts_with(combined_table["depcom"], "97"),
        pc.utf8_slice_codeunits(combined_table["depcom"], 0, 3),
        pc.utf8_slice_codeunits(combined_table["depcom"], 0, 2)
        )
    combined_table = combined_table.append_column("dep", dep)
    filepath_out = filepath_in.replace(".parquet", "") + ".geocoded.parquet"
    pq.write_to_dataset(combined_table, root_path=filepath_out, partition_cols=["dep"])

    return filepath_out


def post_to_addok(filename, filelike_object, requests_options):
    files = {'data': (filename, filelike_object)}
    response = requests.post(ADDOK_URL, files=files, data=requests_options, stream=True)

    # Extract filename from headers
    content_disposition = response.headers.get('content-disposition', '')
    filename = content_disposition[len('attachment; filename="'):-len('"')]

    return filename, response


if __name__ == "__main__":

    # API endpoint for bulk geocoding
    ADDOK_URL = 'http://api-adresse.data.gouv.fr/search/csv/'

    # Geocoding
    start = time.time()
    output_filepath = geocode_bulk(filepath_in="data/adresses_ril_achille.parquet",
                                   requests_options={
                                       "citycode": "depcom",
                                       "columns": "adresse",
                                       "result_columns": ["result_id", "result_name",
                                                          "result_score", "result_type"]
                                   },
                                   n_rows_per_batch=1e6,
                                   sleeptime_between_batches=60
                                   )
    end = time.time()
    logging.info(f"Geocoding done in {end - start} seconds.")
    logging.info(f"Geocoded data saved to {output_filepath}")

    # Checks
    df_check = ds.dataset(output_filepath, format="parquet")
    logging.info(f"Schéma of output file {output_filepath}: \n" + str(df_check.schema))
    logging.info(f"Number of rows: {df_check.count_rows()}")
    logging.info(f"First rows: \n{df_check.head(5).to_pandas()}")
