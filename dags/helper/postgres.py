from airflow.providers.postgres.hooks.postgres import PostgresHook  # Import Airflow's Postgres hook to interact with PostgreSQL
import pandas as pd  # Import pandas to handle data in DataFrame format
BASE_PATH = "/opt/airflow/dags"  # Base path for locating query files in Airflow DAGs folder


class Execute:
    @staticmethod
    def _query(connection_id, query_path):
        """
        Executes a query from a specified SQL file on the provided PostgreSQL connection.

        Args:
            connection_id (str): Airflow connection ID for PostgreSQL.
            query_path (str): Path to the SQL query file.
        """
        # Set up the PostgreSQL connection using Airflow's PostgresHook
        hook = PostgresHook(postgres_conn_id=connection_id)
        connection = hook.get_conn()
        cursor = connection.cursor()

        # Read the SQL query from the file at the specified path
        with open(f'{BASE_PATH}/{query_path}', 'r') as file:
            query = file.read()  # Read the contents of the SQL file

        # Execute the query
        cursor.execute(query)
        cursor.close()  # Close the cursor
        connection.commit()  # Commit the transaction
        connection.close()  # Close the connection

    @staticmethod
    def _get_dataframe(connection_id, query_path):
        """
        Executes a query from a specified SQL file and returns the result as a pandas DataFrame.

        Args:
            connection_id (str): Airflow connection ID for PostgreSQL.
            query_path (str): Path to the SQL query file.

        Returns:
            pd.DataFrame: DataFrame containing the query result.
        """
        # Set up the PostgreSQL connection using Airflow's PostgresHook
        pg_hook = PostgresHook(postgres_conn_id=connection_id)
        connection = pg_hook.get_conn()
        cursor = connection.cursor()

        # Read the SQL query from the file
        with open(f'{BASE_PATH}/{query_path}', 'r') as file:
            query = file.read()  # Read the contents of the SQL file

        # Execute the query and fetch the results
        cursor.execute(query)
        result = cursor.fetchall()  # Fetch all rows from the query result
        column_list = [desc[0] for desc in cursor.description]  # Get column names from the cursor description

        # Create a pandas DataFrame from the query result
        df = pd.DataFrame(result, columns=column_list)

        # Clean up the database connection
        cursor.close()
        connection.commit()  # Commit the transaction (if applicable)
        connection.close()  # Close the connection

        return df  # Return the DataFrame containing the query result

    @staticmethod
    def _insert_dataframe(connection_id, query_path, dataframe):
        """
        Inserts data from a pandas DataFrame into the PostgreSQL database by executing a query for each row.

        Args:
            connection_id (str): Airflow connection ID for PostgreSQL.
            query_path (str): Path to the SQL insert query file.
            dataframe (pd.DataFrame): The pandas DataFrame containing the data to insert.
        """
        # Set up the PostgreSQL connection using Airflow's PostgresHook
        pg_hook = PostgresHook(postgres_conn_id=connection_id)
        connection = pg_hook.get_conn()
        cursor = connection.cursor()

        # Read the SQL query from the file
        with open(f'{BASE_PATH}/{query_path}', 'r') as file:
            query = file.read()  # Read the contents of the SQL insert query file

        # Iterate over each row in the DataFrame and insert it into the database
        for index, row in dataframe.iterrows():
            record = row.to_dict()  # Convert the row to a dictionary to map column names to values
            pg_hook.run(query, parameters=record)  # Execute the insert query with the row data

        # Clean up the database connection
        cursor.close()
        connection.commit()  # Commit the transaction after all inserts
        connection.close()  # Close the connection
