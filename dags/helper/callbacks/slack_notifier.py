from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator  # Import SlackWebhookOperator for sending messages to Slack
import traceback as tb  # Import traceback module to format error messages

def slack_notifier(context):
    """
    Notifies Slack when a task fails, sending details about the task and error information.

    Args:
        context (dict): The context of the Airflow task, which contains information such as task instance, execution date, etc.
    """
    
    # Define the Slack icon to use in the message based on task status (Red circle for failure, could be customized)
    slack_icon = "red_circle"
    
    # Retrieve task state, task ID, DAG ID, execution date, log URL, exception details, and traceback from context
    task_state = context.get('task_instance').state  # The current state of the task (e.g., failed, succeeded)
    task_id = context.get('task_instance').task_id  # The ID of the task that triggered the notification
    dag_id = context.get('task_instance').dag_id  # The ID of the DAG in which the task is running
    task_exec_date = context.get('execution_date')  # The execution date for the task instance
    task_log_url = context.get('task_instance').log_url  # URL to the task logs in Airflow
    
    # If an exception occurred during task execution, retrieve it; otherwise, set it as 'No exception'
    exception = context.get('exception') if context.get('exception') else 'No exception'
    
    # If an exception occurred, format the traceback for detailed debugging information
    traceback = "".join(tb.format_exception(type(exception), exception, exception.__traceback__)) if context.get('exception') else 'No traceback'
    
    # Prepare the message to send to Slack
    slack_msg = f"""
            :{slack_icon}: *Task {task_state}*  # Task status (e.g., failed)
            *Dag*: `{dag_id}`  # DAG ID
            *Task*: `{task_id}`  # Task ID
            *Execution Time*: `{task_exec_date}`  # Time when the task was executed
            *Log Url*: `{task_log_url}`  # URL to the task logs
            *Exceptions*: ```{exception}```  # Exception message
            *Traceback*: ```{traceback}```  # Detailed traceback information if an exception occurred
            """
    
    # Create and configure the Slack webhook task to send the message to the specified Slack channel
    slack_webhook_task = SlackWebhookOperator(
        slack_webhook_conn_id='slack_webhook',  # Connection ID for the Slack webhook in Airflow's connections
        task_id='slack_notification',  # Task ID for the Slack notification task
        message=slack_msg,  # The message content to send to Slack
        channel="pacflight-notifications"  # Slack channel to send the message to
    )
    
    # Execute the Slack notification task, passing the context to the operator
    slack_webhook_task.execute(context=context)  # Trigger the Slack notification with the formatted message
