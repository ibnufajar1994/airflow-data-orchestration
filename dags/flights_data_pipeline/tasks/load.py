from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowSkipException, AirflowException
from helper.minio import CustomMinio
from pangres import upsert
from sqlalchemy import create_engine
from datetime import timedelta
import logging
import pandas as pd


class Load:
    @staticmethod
    def _pacflight_db(table_name, incremental, **kwargs):
        """
        Load data from CSV in MinIO to staging PostgreSQL DB.
        """
        logging.info(f"[Load] Starting load for table: {table_name}")
        date = kwargs.get("ds")  # Get the execution date (used to construct file name)
        ti = kwargs["ti"]  # TaskInstance to get data passed between tasks

        # Retrieve the extraction status from the previous task via XCom
        extract_result = ti.xcom_pull(task_ids=f"extract.{table_name}")
        logging.info(f"[Load] Extract result for {table_name}: {extract_result}")

        # Skip the load step if the extraction was not successful
        if not extract_result or extract_result.get("status") != "success":
            logging.info(f"[Load] Skipping {table_name} due to extract status: {extract_result}")
            raise AirflowSkipException(f"[Load] Skipped {table_name} karena tidak ada data dari extract.")

        table_pkey = kwargs.get("table_pkey")  # Get the primary key for the table
        object_date = (pd.to_datetime(date) - timedelta(days=1)).strftime("%Y-%m-%d")  # Set the date for incremental load
        object_name = f"/temp/{table_name}-{object_date}.csv" if incremental else f"/temp/{table_name}.csv"  # Object name for the file
        bucket_name = "extracted-data"  # MinIO bucket where the data is stored

        engine = create_engine(PostgresHook(postgres_conn_id="warehouse_pacflight").get_uri())  # PostgreSQL engine for staging DB

        try:
            # Download the CSV from the MinIO bucket
            logging.info(f"[Load] Downloading {object_name} from bucket {bucket_name}")
            df = CustomMinio._get_dataframe(bucket_name, object_name)  # Load the data into a DataFrame

            # If the DataFrame is empty, skip the load
            if df.empty:
                logging.warning(f"[Load] Dataframe is empty for {table_name}. Skipping.")
                ti.xcom_push(key="return_value", value={"status": "skipped", "data_date": date})                
                raise AirflowSkipException(f"[Load] Skipping {table_name}: CSV is empty")

            # Set the primary key index for the DataFrame (specific to each table)
            df = df.set_index(table_pkey[table_name])

            # Perform upsert (insert or update) operation into the staging PostgreSQL DB
            upsert(
                con=engine,
                df=df,
                table_name=table_name,
                schema="stg",  # Use the 'stg' schema in PostgreSQL
                if_row_exists="update"  # Update the existing rows if they exist
            )

            # Log success and push the result to XCom for the next task
            logging.info(f"[Load] Load success for {table_name}, {len(df)} records inserted/updated.")
            ti.xcom_push(key="return_value", value={"status": "success", "data_date": date})            

        except AirflowSkipException as e:
            logging.warning(str(e))  # Log warning if the task was skipped
            raise e  # Re-raise the exception to propagate the skip

        except Exception as e:
            logging.error(f"[Load] Failed to load {table_name}: {str(e)}")  # Log any errors that occur
            raise AirflowException(f"[Load] Failed to load {table_name}: {str(e)}")  # Raise an AirflowException if loading fails

        finally:
            engine.dispose()  # Close the engine connection to the PostgreSQL database
