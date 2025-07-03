from airflow.decorators import dag, task_group
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime
from helper.callbacks.slack_notifier import slack_notifier
from pendulum import datetime

from flights_data_pipeline.tasks.extract import Extract
from flights_data_pipeline.tasks.load import Load
from flights_data_pipeline.tasks.transform import Transform

# Default arguments for the DAG, such as failure callback for Slack notifications
default_args = {
    'on_failure_callback': slack_notifier  # Notify on failure via Slack
}

# ========== Task Groups ==========

# Task group for the extract phase of the pipeline
@task_group(group_id="extract")
def extract_group(table_list):
    # Check if incremental data extraction is enabled via Airflow Variables
    incremental = Variable.get("incremental").lower() == "true"
    
    # Iterate through the provided list of tables to extract data for
    for table in table_list:
        # Create a PythonOperator for each table to perform the extraction
        PythonOperator(
            task_id=f"{table}",  # The task ID for each table extraction
            python_callable=Extract._pacflight_db,  # Call the static method for data extraction
            op_kwargs={'table_name': table, 'incremental': incremental},  # Pass table name and incremental flag
            provide_context=True,  # Allow the task to access context from upstream tasks
            do_xcom_push=True  # Push the output to XCom for communication between tasks
        )

# Task group for the load phase of the pipeline
@task_group(group_id="load")
def load_group(tables_with_pkey):
    # Check if incremental data load is enabled via Airflow Variables
    incremental = Variable.get("incremental").lower() == "true"
    load_tasks = []  # List to hold all load tasks

    for table, pkey in tables_with_pkey.items():
        # Create a PythonOperator for each table to load data
        task = PythonOperator(
            task_id=f"{table}",  # The task ID for each table load
            python_callable=Load._pacflight_db,  # Call the static method for data loading
            op_kwargs={
                'table_name': table,
                'incremental': incremental,
                'table_pkey': tables_with_pkey  # Pass the table primary keys for the load task
            },
            provide_context=True,  # Allow the task to access context from upstream tasks
            trigger_rule=TriggerRule.NONE_FAILED  # Ensure task runs only if no previous task failed
        )
        load_tasks.append(task)

    # Set sequential load order due to foreign key dependencies between tables
    for i in range(1, len(load_tasks)):
        load_tasks[i - 1] >> load_tasks[i]  # Enforce the dependency flow between load tasks

# Task group for the transform phase of the pipeline
@task_group(group_id="transform")
def transform_group(transform_tables):
    from airflow.models.baseoperator import chain

    previous = None  # Variable to keep track of the previous task for chaining
    for table in transform_tables:
        # Build the transformation task using the Transform class's build_operator method
        transform = Transform.build_operator(
            task_id=f"{table}",  # Task ID for each transformation task
            table_name=table,  # Name of the table to transform
            sql_dir="flights_data_pipeline/query/final"  # Directory where the SQL queries are stored
        )

        transform.trigger_rule = TriggerRule.NONE_FAILED  # Ensure task runs only if no previous task failed

        # Chain the transformation tasks sequentially, ensuring each task depends on the previous one
        if previous:
            previous >> transform
        previous = transform  # Update previous to the current task for the next iteration

# ========== Main DAG ==========

@dag(
    dag_id='flights_data_pipeline',  # Unique identifier for the DAG
    start_date=datetime(2025, 1, 1),  # Start date for scheduling
    schedule_interval='@daily',  # Set to run once every day
    catchup=True,  # Run for all missed intervals if needed
    max_active_runs=1,  # Limit to one active DAG run at a time
    default_args=default_args,  # Use the default arguments defined above
    tags=['pacflight', 'ETL']  # Tags for categorizing the DAG
)
def flights_data_pipeline():

    # Fetch the list of tables to extract and load from Airflow Variables
    tables_to_extract = eval(Variable.get("tables_to_extract"))
    tables_to_load = eval(Variable.get("tables_to_load"))

    # Define tables for the transformation phase
    transform_tables = [ 
        'dim_aircraft', 'dim_airport', 'dim_seat', 'dim_passenger',
        'fct_boarding_pass', 'fct_booking_ticket',
        'fct_seat_occupied_daily', 'fct_flight_activity'
    ]

    # Run task groups
    extract = extract_group(tables_to_extract)  # Run the extract task group
    load = load_group(tables_to_load)  # Run the load task group
    transform = transform_group(transform_tables)  # Run the transform task group

    # Set the dependency flow: extraction must run before loading, and loading must run before transforming
    extract >> load >> transform  # Ensure extraction -> loading -> transformation order

# Initialize and run the DAG
flights_data_pipeline()
