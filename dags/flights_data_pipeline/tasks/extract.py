from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.providers.postgres.hooks.postgres import PostgresHook
from helper.minio import CustomMinio
import logging
import pandas as pd
import json
from airflow.models import Variable
from datetime import timedelta


class Extract:
    @staticmethod
    def _pacflight_db(table_name, incremental, **kwargs):
        """
        Retrieve all data from the Pacflight database (non-incremental).
        
        Parameters:
            table_name (str): The name of the table from which to extract data.
            **kwargs: Other optional keyword arguments.

        Exceptions:
            AirflowException: Raised if there is an error during data extraction from the Pacflight database.
            AirflowSkipException: Raised if no data is found.
        """
        logging.info(f"[Extract] Starting extraction for table: {table_name}")

        ti = kwargs["ti"]  # TaskInstance to pass information between tasks
        ds = kwargs["ds"]  # The execution date of the task (can be used for filtering)

        try:
            # Initialize connection to the PostgreSQL database using Airflow's PostgresHook
            pg_hook = PostgresHook(postgres_conn_id='pacflight_db')
            connection = pg_hook.get_conn()  # Get the database connection
            cursor = connection.cursor()

            # Define query based on incremental flag (whether to fetch data for only the latest day or all data)
            query = ""
            if incremental:
                date = kwargs['ds']
                query = f"""
                    SELECT * FROM bookings.{table_name}
                    WHERE created_at::DATE = (CAST('{date}' AS DATE) - INTERVAL '1 DAY')
                    OR updated_at::DATE = (CAST('{date}' AS DATE) - INTERVAL '1 DAY');
                """
                object_name = f'/temp/{table_name}-{(pd.to_datetime(date) - timedelta(days=1)).strftime("%Y-%m-%d")}.csv'  # Generating object name for incremental data
            else:
                query = f"SELECT * FROM bookings.{table_name};"  # Fetch all data if not incremental
                object_name = f'/temp/{table_name}.csv'  # Default object name

            logging.info(f"[Extract] Executing query: {query}")
            cursor.execute(query)  # Execute the query
            result = cursor.fetchall()  # Fetch all rows from the query result

            # Get the column names from the cursor description
            column_list = [desc[0] for desc in cursor.description]
            cursor.close()  # Close cursor after execution
            connection.commit()  # Commit the transaction
            connection.close()  # Close the database connection

            # Convert query result to a pandas DataFrame
            df = pd.DataFrame(result, columns=column_list)

            # If the DataFrame is empty, skip the task and log the warning
            if df.empty:
                logging.warning(f"[Extract] Table {table_name} is empty. Skipping...")
                ti.xcom_push(key="return_value", value={"status": "skipped", "data_date": ds})
                raise AirflowSkipException(f"[Extract] Skipped {table_name} — no new data.")

            # Handle JSON columns that need to be dumped as string for specific tables
            if table_name == 'aircrafts_data':
                df['model'] = df['model'].apply(lambda x: json.dumps(x) if pd.notnull(x) else None)

            if table_name == 'airports_data':
                df['airport_name'] = df['airport_name'].apply(lambda x: json.dumps(x) if pd.notnull(x) else None)
                df['city'] = df['city'].apply(lambda x: json.dumps(x) if pd.notnull(x) else None)

            if table_name == 'tickets':
                df['contact_data'] = df['contact_data'].apply(lambda x: json.dumps(x) if x else None)

            # Replace NaN with None in 'flights' table for better compatibility with CSV
            if table_name == 'flights':
                df = df.replace({float('nan'): None})

            # Define the bucket name for MinIO
            bucket_name = 'extracted-data'
            logging.info(f"[Extract] Writing data to MinIO bucket: {bucket_name}, object: {object_name}")
            CustomMinio._put_csv(df, bucket_name, object_name)  # Upload data to MinIO

            result = {"status": "success", "data_date": ds}  # Prepare success result
            logging.info(f"[Extract] Extraction completed for table: {table_name}")
            return result  # Return success result

        except AirflowSkipException as e:
            logging.warning(f"[Extract] Skipped extraction for {table_name}: {str(e)}")
            raise e  # Raise exception if data extraction is skipped
        
        except Exception as e:
            logging.error(f"[Extract] Failed extracting {table_name}: {str(e)}")
            raise AirflowException(f"Error when extracting {table_name} : {str(e)}")  # Raise exception if extraction fails
