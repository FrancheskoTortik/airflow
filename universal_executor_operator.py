# -*- coding: utf-8 -*-
import os

import jinja2
from airflow.exceptions import AirflowSkipException
from airflow.models import BaseOperator, DagRun
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow_extensions.airflow_parameters import RESOURCE_DIR
from airflow_extensions.constants import LOCAL_TZ
from airflow_extensions.hooks.clickhouse_hook import ClickhouseHook
from airflow_extensions.hooks.exasol_hook import ExasolHook
from croniter import croniter

jinja_env = jinja2.Environment(loader=jinja2.FileSystemLoader(os.path.join(RESOURCE_DIR, 'universal_executor')))


class UniversalExecutorOperator(BaseOperator):
    """
    Class for execute query in PostgreSQL, Exasol and Clickhouse databases
    """

    def __init__(self, config, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.sql = config.get('name')
        self._conn_type = config.get('conn_type')
        self._conn_id = config.get('conn_id')
        self.config = config
        self._hook = None
        self._dag_id = ''
        self._task_id = ''

    # pylint: disable=too-many-locals
    def pre_execute(self, context):
        if self._conn_type == 'postgresql':
            self._hook = PostgresHook(self._conn_id)
        elif self._conn_type == 'exasol':
            self._hook = ExasolHook(self._conn_id)
        elif self._conn_type == 'clickhouse':
            self._hook = ClickhouseHook(self._conn_id)
        else:
            raise Exception(
                f"Unsupported connection type. Conn type mast be postgresql, exasol or clickhouse. "
                f"You given {self._conn_type}"
            )

    def get_task_status(self) -> str:
        """
        Method for determining the status of a previous run
        Returns:
            Status of the last dag run for the given dag_id
        """
        last_dag_run = DagRun.find(dag_id=self._dag_id)
        last_dag_run.sort(key=lambda x: x.execution_date, reverse=True)
        if len(last_dag_run) == 1:
            task_inst = last_dag_run[0].get_task_instance(self._task_id)
        else:
            task_inst = last_dag_run[1].get_task_instance(self._task_id)
        return str(task_inst).rsplit(' ', maxsplit=1)[-1][1:-2]

    def _gen_sql_from_template(self, **kwargs):
        """
        Method for render sql query from template
        Args:
            templates - file name with sql query's
        Return:
            Generator sql query
        """
        template_file = jinja_env.get_template(f'{self.sql}.sql.jinja2')
        for rendered_sql in template_file.render(**kwargs).strip(' ;').split(';'):
            yield rendered_sql.strip(' ;')

    def execute(self, context):
        run_date = context.get('data_interval_end').astimezone(LOCAL_TZ)
        self._dag_id = str(context['dag']).split(' ')[1][:-1]
        self._task_id = str(context['task']).split(' ')[1][:-1]
        prev_run_status = self.get_task_status()
        if prev_run_status == 'failed':
            raise Exception("Previous task run failed")

        if self.config.get('schedule'):
            schedule = self.config.get('schedule')
            if croniter.match(schedule, run_date):
                self.log.info("Task schedule match with dag ran")
            else:
                raise AirflowSkipException("Task schedule and task run date not match.")
        else:
            self.log.info("Task is not scheduled individually")

        query_params = {
            'air_date_now': self.config['air_date'].to_datetime_string(),
            'air_date_start_interval': context.get('data_interval_start').astimezone(LOCAL_TZ).to_datetime_string(),
            'air_date_end_interval': context.get('data_interval_end').astimezone(LOCAL_TZ).to_datetime_string(),
        }
        sql_templates = self._gen_sql_from_template(**query_params)
        for sql in sql_templates:
            self._hook.run(sql)
