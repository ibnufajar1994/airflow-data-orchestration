from airflow.hooks.base import BaseHook  # Import Airflow's BaseHook to get connection details
from minio import Minio  # Import the Minio client to interact with MinIO object storage
from io import BytesIO  # Import BytesIO to handle in-memory byte streams
import pandas as pd  # Import pandas for DataFrame handling
import json  # Import json for JSON serialization and deserialization

class MinioClient:
    @staticmethod
    def _get():
        """
        Retrieve the MinIO client using Airflow connection details.
        
        Returns:
            Minio: A Minio client object to interact with the MinIO server.
        """
        minio = BaseHook.get_connection('minio')  # Fetch the connection details for 'minio' from Airflow
        client = Minio(
            endpoint = minio.extra_dejson['endpoint_url'],  # Extract the endpoint URL from connection's extra JSON
            access_key = minio.login,  # Access key from Airflow connection
            secret_key = minio.password,  # Secret key from Airflow connection
            secure = False  # Set to False, as it's not using SSL/TLS
        )

        return client  # Return the initialized Minio client
    
class CustomMinio:
    @staticmethod
    def _put_csv(dataframe, bucket_name, object_name):
        """
        Upload a DataFrame as a CSV to the specified MinIO bucket.

        Args:
            dataframe (pd.DataFrame): The DataFrame to upload.
            bucket_name (str): The MinIO bucket to store the file.
            object_name (str): The object name (file name) in the bucket.
        """
        # Convert the DataFrame to CSV in memory (as bytes)
        csv_bytes = dataframe.to_csv(index=False).encode('utf-8')
        csv_buffer = BytesIO(csv_bytes)  # Convert CSV bytes to a buffer

        # Get the MinIO client
        minio_client = MinioClient._get()
        
        # Upload the CSV data to MinIO
        minio_client.put_object(
            bucket_name = bucket_name,
            object_name = object_name,
            data = csv_buffer,  # Data to upload (in memory)
            length = len(csv_bytes),  # Length of the CSV data in bytes
            content_type = 'application/csv'  # MIME type of the file
        )

    @staticmethod
    def _put_json(json_data, bucket_name, object_name):
        """
        Upload a JSON object to the specified MinIO bucket.

        Args:
            json_data (dict): The JSON data to upload.
            bucket_name (str): The MinIO bucket to store the file.
            object_name (str): The object name (file name) in the bucket.
        """
        # Convert JSON data to a JSON string and then to bytes
        json_string = json.dumps(json_data)
        json_bytes = json_string.encode('utf-8')
        json_buffer = BytesIO(json_bytes)  # Convert JSON bytes to a buffer

        # Get the MinIO client
        minio_client = MinioClient._get()

        # Upload the JSON data to MinIO
        minio_client.put_object(
            bucket_name = bucket_name,
            object_name = object_name,
            data = json_buffer,  # Data to upload (in memory)
            length = len(json_bytes),  # Length of the JSON data in bytes
            content_type = 'application/json'  # MIME type of the file
        )

    @staticmethod
    def _get_dataframe(bucket_name, object_name):
        """
        Retrieve a CSV file from MinIO and convert it into a pandas DataFrame.

        Args:
            bucket_name (str): The MinIO bucket containing the CSV file.
            object_name (str): The object name (file name) in the bucket.

        Returns:
            pd.DataFrame: The CSV data loaded into a pandas DataFrame.
        """
        # Get the MinIO client
        minio_client = MinioClient._get()
        
        # Retrieve the CSV data from MinIO as a file-like object
        data = minio_client.get_object(
            bucket_name = bucket_name,
            object_name = object_name
        )

        # Read the CSV data directly into a pandas DataFrame
        df = pd.read_csv(data)

        return df  # Return the DataFrame
