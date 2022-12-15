from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import ShortCircuitOperator
from airflow_extensions.constants import LOCAL_TZ
from airflow_extensions.operators.notify_operator.notify_events_operator import NotifyEventsOperator
from airflow_extensions.operators.notify_operator.notify_messages_operator import (
    NotifyMessagesOperator,
    notify_messages_skipper,
)
from airflow_extensions.utils.dag_helper import DagHelper
from airflow_extensions.utils.messenger import ErrorDagsMessage, TelegramErrorSender

DAG_ID = 'notify'

helper = DagHelper()
default_args = helper.get_config('dag_default_args')
config = helper.get_config(dag_id=DAG_ID)

default_args['start_date'] = (datetime.strptime(config['start_date'], "%Y-%m-%d %H:%M:%S")).replace(tzinfo=LOCAL_TZ)
error_message = ErrorDagsMessage()
default_args.update({'on_failure_callback': TelegramErrorSender(error_message).send})
default_args.update({'on_retry_callback': TelegramErrorSender(error_message).send})

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    catchup=True,
    schedule_interval='10 * * * *',
    max_active_runs=1,
) as dag:
    events_tasks = []
    for app_id in config['app_ids']:
        notify_events_task = NotifyEventsOperator(
            app_id=app_id,
            base_url=config['base_url'],
            delta=timedelta(minutes=15),
            task_id=f'notify_events_{app_id}',
            depends_on_past=True,
            execution_timeout=timedelta(minutes=15),
            retry_delay=timedelta(minutes=6),
            retries=10,
        )

        events_tasks.append(notify_events_task)
        if len(events_tasks) > 1:
            events_tasks[-2].set_downstream(events_tasks[-1])

    dummy_task = DummyOperator(task_id='dummy_notify_events')
    if events_tasks:
        events_tasks[-1].set_downstream(dummy_task)

    notify_messages_skipper_task = ShortCircuitOperator(
        task_id='notify_messages_skipper',
        python_callable=notify_messages_skipper,
        execution_timeout=timedelta(minutes=1),
        retries=0,
    )

    notify_messages = NotifyMessagesOperator(
        task_id='notify_messages',
        base_url=config['base_url'],
        cluster=config['cluster'],
        app_ids=config['app_ids'],
        execution_timeout=timedelta(minutes=15),
        retries=3,
        retry_delay=timedelta(minutes=3),
    )

    notify_messages.set_downstream(notify_messages_skipper_task)
