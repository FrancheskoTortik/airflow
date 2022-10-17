# -*- coding: utf-8 -*-

import datetime
import os
import re
from dataclasses import dataclass, fields
from urllib.parse import quote

import jinja2
import pendulum
from airflow.exceptions import AirflowBadRequest
from airflow.models import BaseOperator
from airflow_extensions.constants import LOCAL_TZ
from airflow_extensions.exceptions import UnexpectedValueError
from airflow_extensions.hooks.exasol_hook import ExaConnectionExt, ExasolHook
from airflow_extensions.models.storage import get_table_storage
from airflow_extensions.utils.slack import send_message

METADATA_SCHEMA = 'metadata'
DQ_CHECKS_TABLE = 'dq_checks'
DQ_AGG_LOG_TABLE = 'dq_check_log'
DQ_SCHEMA = 'dq'


@dataclass
class DQChecksTableRow:
    """Check description and detailed log table name class"""
    check_desc: str
    detailed_log_table: str
    method_name: str


class DqCheckOperator(BaseOperator):
    def __init__(
        self, schema, table, check_type_cd, resource_dir, *args, use_exec_dt=False, exasol_conn_id='exasol', **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.schema = schema.lower()
        self.table = table.lower()
        self.check_type_cd = check_type_cd
        self.use_exec_dt = use_exec_dt
        self.exasol_conn_id = exasol_conn_id
        self.template_dir = os.path.join(resource_dir, 'templates/details')

    # pylint: disable=too-many-locals
    def execute(self, context):
        exa_hook = ExasolHook(exasol_conn_id=self.exasol_conn_id)
        jinja_env = jinja2.Environment(loader=jinja2.FileSystemLoader(self.template_dir))
        utc_tz = pendulum.timezone('UTC')

        if self.use_exec_dt:
            exec_dttm = context['data_interval_start'].replace(tzinfo=utc_tz).astimezone(LOCAL_TZ)
            log_dttm = exec_dttm.strftime("%Y-%m-%d %H:%M:%S")
        else:
            log_dttm = pendulum.now(LOCAL_TZ).strftime("%Y-%m-%d %H:%M:%S")

        with exa_hook.get_conn() as conn:
            # Get view name, detailed log table name and method name
            sql = f"""
                select view_name_template, detailed_log_table, method_name
                from {METADATA_SCHEMA}.{DQ_CHECKS_TABLE} 
                where check_type_cd = {self.check_type_cd}
            """
            with conn.execute(sql) as stmt:
                check_info = stmt.fetchone()

            check_view_name = check_info[0].format(schema=self.schema, table=self.table).lower()
            detailed_log_table = check_info[1]
            dq_name = check_info[2].lower()

            # Populate detailed check log
            template = jinja_env.get_template(f'{dq_name}.sql.jinja2')
            sql_list = (
                template.render(
                    meta_schema=METADATA_SCHEMA,
                    detailed_log_table=detailed_log_table,
                    dq_schema=DQ_SCHEMA,
                    dq_view=check_view_name,
                    exa_schema=self.schema,
                    exa_table=self.table,
                    is_filtered=None,
                    check_dttm=log_dttm,
                )
                .strip(' \n;')
                .split(';')
            )
            for sql_stmt in sql_list:
                conn.execute(sql_stmt).close()

            # Populate aggregated check log
            sql = f"""
                insert into {METADATA_SCHEMA}.{DQ_AGG_LOG_TABLE}
                (check_dttm, schema_name, table_name, check_type_cd, error_cnt, error_query)
                select
                  '{log_dttm}' as check_dttm,
                  '{self.schema}' as schema_name,
                  '{self.table}' as table_name,
                  {self.check_type_cd} as check_type_cd,
                  count(*) as error_cnt,
                  'select * from {DQ_SCHEMA}.{check_view_name}' as error_query
                from {METADATA_SCHEMA}.{detailed_log_table}
                where check_dttm = '{log_dttm}'
                  and schema_name = '{self.schema}'
                  and table_name = '{self.table}'
                  and not check_passed_flg
                having count(*) > 0
            """
            conn.execute(sql).close()

            conn.commit()


class DqViewGenOperator(BaseOperator):
    allowed_dq_check_methods = ('replica_cnt',
                                'dds_version_check',
                                'dds_duplicates_check',
                                'dds_rows_count_check',
                                'ch_city_import_check')

    # pylint: disable=too-many-arguments
    def __init__(
        self,
        table,
        schema,
        check_type_cd,
        gen_params,
        resource_dir,
        *args,
        view_schema=DQ_SCHEMA,
        exasol_conn_id='exasol',
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.table = table
        self.schema = schema.lower()
        self.view_schema = view_schema
        self.check_type_cd = check_type_cd
        self.parameters = gen_params
        self.exasol_conn_id = exasol_conn_id
        self.template_dir = os.path.join(resource_dir, 'templates/views')

    def pre_execute(self, context):
        # noinspection PyAttributeOutsideInit
        # pylint: disable=attribute-defined-outside-init
        self.jinja_env = jinja2.Environment(loader=jinja2.FileSystemLoader(self.template_dir))

    def execute(self, context):
        exa_hook = ExasolHook(exasol_conn_id=self.exasol_conn_id)
        with exa_hook.get_conn() as conn:
            # Get view name and method by check_type_cd
            sql = f"""
                select view_name_template, method_name 
                from {METADATA_SCHEMA}.{DQ_CHECKS_TABLE} 
                where check_type_cd = {self.check_type_cd}
            """
            with conn.execute(sql) as stmt:
                row = stmt.fetchone()

            view_name = row[0].format(schema=self.schema, table=self.table).lower()
            gen_method = row[1].lower()

            if gen_method not in self.allowed_dq_check_methods:
                raise UnexpectedValueError(
                    f'Only the following dq check methods allowed: {", ".join(self.allowed_dq_check_methods)}')

            # Call gen method
            self.parameters.update({'view': view_name, 'conn': conn})
            getattr(self, gen_method)(**self.parameters)

            conn.commit()

    # pylint: disable=too-many-arguments
    def ch_city_import_check(self, view, conn: ExaConnectionExt, table, schema, date_field, key):
        template = self.jinja_env.get_template('ch_city_import_query.sql.jinja2')
        ch_city_import_query = quote(
            template.render(
                table=table,
                schema=schema,
                key=key,
                date_field=date_field,
            )
        )

        template = self.jinja_env.get_template('ch_city_import_check.sql.jinja2')
        sql = template.render(
            dq_schema=self.view_schema,
            dq_view=view,
            table=table,
            schema=schema,
            key=key,
            date_field=date_field,
            ch_city_import_query=ch_city_import_query,
        )

        conn.execute(sql).close()

    @staticmethod
    def _get_ch_date_column_type(conn: ExaConnectionExt, exa_to_src_conn, src_schema, src_table, date_column):
        sql = f"""
            SELECT DATATYPE
            FROM (
                IMPORT FROM JDBC AT {exa_to_src_conn} STATEMENT '
                SELECT type AS DATATYPE FROM system.columns
                WHERE (database, table, name) = (''{src_schema}'', ''{src_table}'', ''{date_column}'')
                '
            )
        """
        dtype = conn.execute(sql).fetchone()
        if not dtype:
            raise Exception(f'src_table table not found in \'{exa_to_src_conn}\'')

        dtype = dtype[0]
        result = {}
        for dt_type in ('DateTime64', 'DateTime'):
            if dtype.startswith(dt_type):
                result['function'] = 'to' + dt_type
                groups = re.search(r'\(.*\)', dtype)
                if groups:
                    args = re.findall(r"[/\w]+", groups.group(0))
                    if dtype.startswith('DateTime64'):
                        result['accuracy'] = args[0]
                        if len(args) == 2:
                            result['tz'] = args[1]
                    else:
                        result['tz'] = args[0]
                return result
        if dtype.startswith('Date'):
            result['function'] = 'toDate'
        return result

    # pylint: disable=too-many-arguments
    def replica_cnt(self, view, conn: ExaConnectionExt, threshold, exa_to_src_conn, src_schema,
                    src_table=None, src_index=None, date_column=None, where=None):
        if where is None:
            where = []
        schema_to_dbms = {'replica': 'mysql', 'replica_ch': 'clickhouse'}

        if self.schema not in schema_to_dbms:
            raise ValueError(f'Schema {self.schema} is not supported. '
                             f'Allowed replica schemas: {", ".join(schema_to_dbms)}')

        if src_index and schema_to_dbms[self.schema] != 'mysql':
            raise ValueError('Index can only be used for MySQL Source')

        function = {}
        if schema_to_dbms[self.schema] == 'clickhouse' and (src_table or self.table) and date_column:
            function = self._get_ch_date_column_type(
                conn, exa_to_src_conn, src_schema, src_table or self.table, date_column
            )

        template = self.jinja_env.get_template('replica_cnt.sql.jinja2')
        template.globals['datetime'] = datetime
        sql = template.render(
            dq_schema=self.view_schema,
            dq_view=view,
            date_column=date_column,
            src_type=schema_to_dbms[self.schema],
            src_schema=src_schema,
            src_table=src_table or self.table,
            src_index=src_index,
            exa_to_src_conn=exa_to_src_conn,
            exa_schema=self.schema,
            exa_table=self.table.lower(),
            threshold=threshold,
            where=where,
            function=function,
        )
        conn.execute(sql).close()

    # pylint: disable=too-many-arguments
    def dds_version_check(self, view, conn: ExaConnectionExt, table, schema, business_key):
        template = self.jinja_env.get_template('dds_version_check.sql.jinja2')

        sql = template.render(
            dq_schema=self.view_schema,
            dq_view=view,
            table=table,
            schema=schema,
            key=business_key,
        )
        conn.execute(sql).close()

    # pylint: disable=too-many-arguments
    def dds_duplicates_check(self, view, conn: ExaConnectionExt, table, schema, business_key, load_type):
        if load_type == 'SCD2':
            business_key.append('valid_from_dttm')
        template = self.jinja_env.get_template('dds_duplicates_check.sql.jinja2')
        sql = template.render(
            dq_schema=self.view_schema,
            dq_view=view,
            table=table,
            schema=schema,
            key=business_key,
        )
        conn.execute(sql).close()

    # pylint: disable=too-many-arguments, too-many-locals
    def dds_rows_count_check(self, view, conn: ExaConnectionExt, table, schema, business_key, load_type):
        t = get_table_storage().load_table(schema, table)
        exa_filter = 'delete_flg = false' if 'delete_flg' in t.fields else ''

        for source in t.sources:
            src_schema, src_table = source.source_schema, source.source_table_name
            src_filter = source.where
            src_key = [', '.join(t.fields[key].source_columns) for key in business_key]

            template = self.jinja_env.get_template('ch_count_query.sql.jinja2')
            statement = quote(
                template.render(
                    src_schema=src_schema,
                    src_table=src_table,
                    src_key=src_key,
                    src_filter=src_filter,
                )
            )

            template = self.jinja_env.get_template('dds_rows_count_check.sql.jinja2')
            sql = template.render(
                dq_schema=self.view_schema,
                dq_view=view,
                table=table,
                schema=schema,
                key=business_key,
                exa_filter=exa_filter,
                statement=statement,
                load_type=load_type,
            )
            conn.execute(sql).close()


class DqAlertOperator(BaseOperator):
    def __init__(self, check_type_cd, resource_dir, *args, exasol_conn_id='exasol', **kwargs):
        super().__init__(*args, **kwargs)
        self.check_type_cd = check_type_cd
        self.exasol_conn_id = exasol_conn_id
        self.template_dir = os.path.join(resource_dir, 'templates/details')
        self.exa_hook = None

    def get_dq_checks_table_row(self) -> DQChecksTableRow:
        sql = f"""
                select {', '.join(field.name for field in fields(DQChecksTableRow))}
                from {METADATA_SCHEMA}.{DQ_CHECKS_TABLE}
                where check_type_cd = {self.check_type_cd}
                """
        row = self.exa_hook.fetchone_dict(sql, lower_ident=True)
        return DQChecksTableRow(**row)

    def execute(self, context):
        self.exa_hook = ExasolHook(exasol_conn_id=self.exasol_conn_id)
        check_info_row = self.get_dq_checks_table_row()

        jinja_env = jinja2.Environment(loader=jinja2.FileSystemLoader(self.template_dir))
        template = jinja_env.get_template(f'{check_info_row.method_name}_alert.sql.jinja2')
        sql = template.render(
            meta_schema=METADATA_SCHEMA,
            detailed_log_table=check_info_row.detailed_log_table
        )
        table_report = self.exa_hook.get_pretty_table(sql)
        table_report.align = 'l'

        # pylint: disable=protected-access
        if len(table_report._rows) == 0:
            params = {'message': f'{check_info_row.check_desc} — ошибок не найдено!',
                      'icon_emoji': ':white_check_mark:'}
        else:
            params = {'message': f'{check_info_row.check_desc}\n```{table_report.get_string()}```',
                      'icon_emoji': ':japanese_ogre:'}

        response = send_message(channel_name='dwh-notification', **params)

        if not response.ok:
            # TODO: Should think about our custom exception
            raise AirflowBadRequest(response.text)
