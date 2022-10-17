import os
import re
import tempfile
from io import BytesIO
from tempfile import NamedTemporaryFile

import aiohttp
import cchardet
import pandas as pd
import pendulum
from airflow.exceptions import AirflowSkipException
from airflow.models import BaseOperator
from airflow.models.variable import Variable
from airflow_extensions.airflow_parameters import HOME_DIR
from airflow_extensions.constants import CLICKHOUSE_CLUSTER_NAMES, CLICKHOUSE_SCRAT_CONN_ID, LOCAL_TZ
from airflow_extensions.exceptions import UnexpectedValueError
from airflow_extensions.hooks.clickhouse_hook import ClickhouseHook
from airflow_extensions.operators.ch_table_operator import ChTable
from airflow_extensions.utils import clickhouse
from airflow_extensions.utils.jinja_extensions import get_sql_template
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

TEMPLATE_DIR = os.path.join(HOME_DIR, 'data-flow', 'airflow_extensions', 'resources', 'pg_stg')


class GoogleDriveService:
    def __init__(self):
        creds = Variable.get('google_drive_secret', deserialize_json=True)
        credentials = Credentials.from_authorized_user_info(**creds)
        self.service = build('drive', 'v3', credentials=credentials, cache_discovery=False)

    # pylint: disable=E1101
    def get_folder(self, folder):
        results = (
            self.service.files()
            .list(
                corpora="user",
                q=f"name = '{folder}' and mimeType = 'application/vnd.google-apps.folder'",
                fields="files(id, name)",
            )
            .execute()
        )
        folders = results.get('files', [])

        if not folders:
            raise FileNotFoundError("No folders found.")

        if len(folders) > 1:
            raise AssertionError(f"More than one folder found by name '{self.folder}': {', '.join(folders)}")

        return folders[0]

    # pylint: disable=E1101
    def get_list_files(self, folder_id):
        results = (
            self.service.files()
            .list(corpora="user", q=f"'{folder_id}' in parents", fields="files(id, name, modifiedTime)")
            .execute()
        )
        return results.get('files', [])

    # pylint: disable=E1101
    def move_file(self, file_id, folder_id):
        file = self.service.files().get(fileId=file_id, fields='parents').execute()
        previous_parents = ",".join(file.get('parents'))

        file = (
            self.service.files()
            .update(fileId=file_id, addParents=folder_id, removeParents=previous_parents, fields='id, parents')
            .execute()
        )

    # pylint: disable=E1101
    def get_csv_file(self, file_id, file_name):
        if file_name.endswith('.csv'):
            file_resource = self.service.files().get_media(fileId=file_id).execute()
        else:
            file_resource = (
                self.service.files()
                .export(
                    fileId=file_id,
                    mimeType="text/csv",
                )
                .execute()
            )
        return file_resource


# pylint: disable=too-many-instance-attributes
class GoogleDriveToChStgOperator(BaseOperator):
    ui_color = '#E1FFE4'
    allowed_load_methods = ('append',)

    def __init__(self, params, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.folder = params['folder']
        self.method = params['method']['name']
        self.schema = params['stg_schema']
        self.table = params['table_name']
        self.source_fields = params['source_fields']
        self.target_fields = params['target_fields']
        self.file_name = params.get('file_name')
        self.file_name_pattern = params.get('file_name_pattern')
        self.file_name_parsing = params.get('file_name_parsing')
        self.folder_for_move = params['folder_for_move']
        self.folder_for_fail = params['folder_for_fail']
        self.drop_index_rows = params.get('drop_index_rows', [])
        self.cluster = CLICKHOUSE_CLUSTER_NAMES[CLICKHOUSE_SCRAT_CONN_ID]
        self.ch_conn_id = CLICKHOUSE_SCRAT_CONN_ID

    # pylint: disable=W0201
    def pre_execute(self, context):
        if self.method not in self.allowed_load_methods:
            raise UnexpectedValueError(
                f'Only the following load methods are allowed: {", ".join(self.allowed_load_methods)}'
            )
        self.ch_hook = ClickhouseHook(conn_id=self.ch_conn_id)
        self.google_drive = GoogleDriveService()

    def execute(self, context):
        files = self.get_files()
        if not files:
            raise AirflowSkipException('No new files, exiting...')

        for file_name, file_id in files.items():
            with NamedTemporaryFile() as temp_file:
                try:
                    data = self.get_tmp_file(file_id, file_name, temp_file)
                except Exception as err:  # pylint: disable=W0703
                    self.google_drive.move_file(file_id, self.get_move_folder_id(self.folder_for_fail))
                    self.log.info(
                        "file '%s' has been moved to a folder '%s', \nwith error: %s",
                        file_name,
                        self.folder_for_fail,
                        str(err),
                    )
                else:
                    self.ch_import_data(data)
                    self.google_drive.move_file(file_id, self.get_move_folder_id(self.folder_for_move))
                    self.log.info("file '%s' has been moved to a folder '%s'", file_name, self.folder_for_move)

    def get_files(self):
        folder_id = self.google_drive.get_folder(self.folder)['id']
        files = {
            file['name']: file['id']
            for file in self.google_drive.get_list_files(folder_id)
            if (self.file_name_pattern and re.match(self.file_name_pattern, file['name'], flags=re.I))
            or (self.file_name and self.file_name == file['name'])
            or (not self.file_name and not self.file_name_pattern)
        }
        return files

    def get_move_folder_id(self, folder_name):
        folder_id = self.google_drive.get_folder(self.folder)['id']
        move_folder_id = [
            file['id'] for file in self.google_drive.get_list_files(folder_id) if file['name'] == folder_name
        ][0]
        return move_folder_id

    def parse_filename(self, filename):
        values = {}
        for field, re_sub in self.file_name_parsing.items():
            value = re.sub(re_sub[0], re_sub[1], filename)
            values[field] = value
        return values

    def prepare_fields(self):
        column_names = clickhouse.get_column_names_with_types(self.ch_hook, self.schema, self.table)
        db_columns = {
            tuple(row.values())[0]: tuple(row.values())[1].lower()
            for row in column_names
            if tuple(row.values())[0] in self.target_fields
        }
        fields_map = {self.target_fields[idx]: field for idx, field in enumerate(self.source_fields)}
        int_cols = [col_name for col_name, col_type in db_columns.items() if 'int' in col_type]
        float_columns = [col_name for col_name, col_type in db_columns.items() if 'float' in col_type]
        int_columns = {}
        for col in int_cols:
            int_columns.update({fields_map[col]: 'Int64'})
        return int_columns, float_columns

    # pylint: disable=no-member, unsupported-assignment-operation, unsubscriptable-object
    def prepare_input_file(self, file, file_name):
        encoding = cchardet.detect(file).get("encoding")
        int_columns, float_columns = self.prepare_fields()
        df_csv = pd.read_csv(
            BytesIO(file),
            sep=';',
            encoding=encoding,
            usecols=lambda c: c in set(self.source_fields),
            dtype=int_columns,
        )
        if df_csv.empty:
            df_csv = pd.read_csv(
                BytesIO(file),
                sep=',',
                encoding=encoding,
                usecols=lambda c: c in set(self.source_fields),
                dtype=int_columns,
            )
        if df_csv.empty:
            raise Exception('DF is empty. Check separator (supports sep: [,;]) or column names on this file.')
        if self.drop_index_rows:
            df_csv.drop(df_csv.index[self.drop_index_rows], inplace=True)
        df_csv = df_csv.dropna(how='all')
        df_csv = df_csv.reindex(columns=self.source_fields)
        df_csv.columns = self.target_fields
        for idx, (key, val) in enumerate(self.parse_filename(file_name).items()):
            df_csv.insert(idx, key, val)
        for f_col in float_columns:
            df_csv[f_col] = [str(x).replace(' ', '').replace('\xa0', '').replace(',', '.') for x in df_csv[f_col]]
            df_csv[f_col] = df_csv[f_col].astype(float)
        return df_csv

    def get_tmp_file(self, file_id, file_name, temp_file):
        file_resource = self.google_drive.get_csv_file(file_id, file_name)
        df_csv = self.prepare_input_file(file_resource, file_name)
        df_csv.to_csv(temp_file, index=False)
        temp_file.seek(0)
        return temp_file

    def ch_import_data(self, data):
        extra_fields = list(field for field in self.file_name_parsing.keys())
        # pylint: disable=W0212
        self.ch_hook.import_data(
            table=f'{self.schema}.{self.table}',
            data=data,
            data_format='CSVWithNames',
            register_payload={'factory': aiohttp.payload.IOBasePayload, 'type': tempfile._TemporaryFileWrapper},
            target_fields=extra_fields + self.target_fields,
        )


class GoogleDriveToChSnpOperator(BaseOperator):
    ui_color = '#E1F4FF'
    allowed_load_methods = ('replace', 'append')

    def __init__(
            self,
            params,
            *args, **kwargs):

        super().__init__(*args, **kwargs)

        self.folder = params['folder']
        self.method = params['method']['name']
        self.table = params['table_name']
        self.source_fields = params['source_fields']
        self.target_fields = params['target_fields']
        self.schema = params['snp_schema']
        self.primary_key = params['primary_key']
        self.file_name = params.get('file_name')
        self.ignore_modified_time = params.get('ignore_modified_time', False)
        self.cluster = CLICKHOUSE_CLUSTER_NAMES[CLICKHOUSE_SCRAT_CONN_ID]
        self.ch_conn_id = CLICKHOUSE_SCRAT_CONN_ID
        self.is_distributed = False
        self.temp_table = None
        self.etl_schema = 'etl'

    # pylint: disable=E1120
    def load_to_snp(self):
        self.is_distributed = clickhouse.get_engine(self.ch_hook, self.schema, self.table) == 'Distributed'
        folder_id = self.google_drive.get_folder(self.folder)['id']
        last_processed_dttm = self.get_last_processed_dttm()
        drive_files = {
            file['name']: {'id': file['id'], 'modifiedTime': pendulum.parse(file['modifiedTime']).astimezone(LOCAL_TZ)}
            for file in self.google_drive.get_list_files(folder_id)
            if pendulum.parse(file['modifiedTime']).astimezone(LOCAL_TZ).naive() > last_processed_dttm
            or self.ignore_modified_time
        }

        if not drive_files:
            raise AirflowSkipException('No new files, exiting...')
        elif self.method == 'replace':
            table = f"_{self.table}" if self.is_distributed else self.table
            if not clickhouse.is_table_empty(self.ch_hook, self.schema, self.table):
                clickhouse.truncate(self.ch_hook, self.schema, table, self.cluster)
            self.insert_csv(drive_files)
        elif self.method == 'append':
            self.temp_table = f"{self.table}__tmp_{self.data_interval_start}"

            self.create_temp_snp_table()

            self.insert_csv(drive_files)

            self.delete_from_snp_repeat_pk()

            self.insert_to_snp_from_tmp()

            ChTable.drop_table(self.ch_hook, self.etl_schema, self.temp_table, self.cluster, self.log)

    def get_last_processed_dttm(self):
        sql = f"""
        select max(processed_dttm) 
        from {self.schema}.{self.table} 
        """
        max_date = self.ch_hook.get_val(sql)
        return max_date

    # pylint: disable=E1101
    def get_tmp_file(self, file_id, file_name, temp_file):
        file_resource = self.google_drive.get_csv_file(file_id, file_name)
        int_columns = {}
        columns = clickhouse.get_column_names_with_types(self.ch_hook, self.schema, self.table)
        columns = [tuple(row.values())[0] for row in columns if 'int' in tuple(row.values())[1].lower()]
        fields_map = {self.target_fields[idx]: field for idx, field in enumerate(self.source_fields)}
        for col in columns:
            int_columns.update({fields_map[col]: 'Int64'})
        df_csv = pd.read_csv(BytesIO(file_resource), usecols=self.source_fields, dtype=int_columns)
        df_csv = df_csv.dropna(how='all')
        df_csv.columns = self.target_fields
        df_csv.to_csv(temp_file, index=False)
        temp_file.seek(0)

        return temp_file

    def create_temp_snp_table(self):
        columns = clickhouse.get_column_names_with_types(self.ch_hook, self.schema, self.table)
        columns = dict([tuple(row.values()) for row in columns if tuple(row.values())[0] in self.target_fields])
        ChTable.create_or_replace_table(
            self.ch_hook,
            self.etl_schema,
            self.temp_table,
            columns,
            self.primary_key,
            self.cluster,
            self.log,
        )

    def delete_from_snp_repeat_pk(self):
        table = f"_{self.table}" if self.is_distributed else self.table
        where = (
            f"({', '.join(self.primary_key)}) IN "
            f"(SELECT {', '.join(self.primary_key)} FROM {self.etl_schema}.{self.temp_table})"
        )
        clickhouse.delete_if_exists(self.ch_hook, self.schema, table, where, self.cluster)

    def insert_to_snp_from_tmp(self):
        template_params = {
            'src_schema': self.etl_schema,
            'src_table': self.temp_table,
            'src_columns': ', '.join(self.target_fields),
            'trg_schema': self.schema,
            'trg_table': self.table,
            'trg_columns': ', '.join(self.target_fields),
            'where': '',
        }
        self.ch_hook.run(
            get_sql_template(
                template_dir=TEMPLATE_DIR,
                block_name='ch_insert_from_temp_tb',
                template_params=template_params,
                template_name='ch_queries',
            ),
            distributed_product_mode='local',
            insert_distributed_sync=1,
            parallel_distributed_insert_select=2,
        )
        clickhouse.wait_replication(self.ch_hook, self.schema, self.table, self.cluster)

    def ch_import_data(self, data):
        table = self.temp_table or self.table
        schema = self.schema if not self.temp_table else self.etl_schema
        self.ch_hook.import_data(
            table=f'{schema}.{table}',
            data=data,
            data_format='CSVWithNames',
            register_payload={'factory': aiohttp.payload.IOBasePayload, 'type': tempfile._TemporaryFileWrapper},
            target_fields=self.target_fields,
        )
        if self.is_distributed or self.temp_table:
            clickhouse.wait_replication(self.ch_hook, schema, table, self.cluster)

    def insert_csv(self, drive_files):
        for f_name, f_info in drive_files.items():
            if self.file_name:
                if self.file_name == f_name:
                    with NamedTemporaryFile() as temp_file:
                        data = self.get_tmp_file(f_info['id'], f_name, temp_file)
                        self.ch_import_data(data)
            else:
                with NamedTemporaryFile() as temp_file:
                    data = self.get_tmp_file(f_info['id'], f_name, temp_file)
                    self.ch_import_data(data)

    # pylint: disable=W0201
    def pre_execute(self, context):
        if self.method not in self.allowed_load_methods:
            raise UnexpectedValueError(
                f'Only the following load methods are allowed: {", ".join(self.allowed_load_methods)}')
        self.data_interval_start = context['data_interval_start'].strftime('%y%m%d_%H%M%S')
        self.ch_hook = ClickhouseHook(conn_id=self.ch_conn_id)
        self.google_drive = GoogleDriveService()

    def execute(self, context):
        self.load_to_snp()
