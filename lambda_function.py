import awswrangler as wr
import pandas as pd
import urllib.parse
import os
import logging

# Initialize logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Environment Variables
os_input_s3_cleansed_layer = os.environ.get('s3_cleansed_layer')
os_input_glue_catalog_db_name = os.environ.get('glue_catalog_db_name')
os_input_glue_catalog_table_name = os.environ.get('glue_catalog_table_name')
os_input_write_data_operation = os.environ.get('write_data_operation')

# Check if all required environment variables are set
if not all([os_input_s3_cleansed_layer, os_input_glue_catalog_db_name, os_input_glue_catalog_table_name, os_input_write_data_operation]):
    raise ValueError("One or more environment variables are missing.")

def lambda_handler(event, context):
    # Extract bucket and object key from event
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')

    try:
        # Load JSON data from S3
        logger.info(f"Reading JSON file from s3://{bucket}/{key}")
        df_raw = wr.s3.read_json(f's3://{bucket}/{key}')

        # Check if 'items' key exists in the JSON data
        if 'items' not in df_raw:
            raise ValueError("'items' key not found in JSON data")

        # Normalize JSON data
        logger.info("Normalizing JSON data")
        df_step_1 = pd.json_normalize(df_raw['items'])

        # Write DataFrame to Parquet and update Glue Catalog
        logger.info(f"Writing data to S3 path: {os_input_s3_cleansed_layer}")
        wr_response = wr.s3.to_parquet(
            df=df_step_1,
            path=os_input_s3_cleansed_layer,
            dataset=True,
            database=os_input_glue_catalog_db_name,
            table=os_input_glue_catalog_table_name,
            mode=os_input_write_data_operation
        )

        logger.info("Data successfully written to S3 and Glue catalog updated")
        return wr_response

    except Exception as e:
        logger.error(f"Error processing file {key} from bucket {bucket}: {e}")
        raise e
