import gc
import json
import re
import time
from datetime import datetime, timedelta
from typing import Any
from urllib.parse import urljoin

import requests
from airflow.models import BaseOperator, Connection
from airflow_extensions.constants import CLICKHOUSE_SCRAT_CONN_ID, LOCAL_TZ
from airflow_extensions.hooks.clickhouse_hook import ClickhouseHook
from airflow_extensions.utils import clickhouse


# pylint: disable=R0902
class ParkomaticaDataLoaderToCH(BaseOperator):
    """
    Class for load data from parkomatica API
    """

    _ch_hook: ClickhouseHook
    token_api: str

    def __init__(self, config, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.config = config
        self._database = config['database_name']
        self._table = config['name']
        self.base_url = config['base_url']
        self.counter = 0
        self.status = config.get('status', '')
        self._organization_id = config['organization_id']
        self._method = config['method']['name']
        self._append_field = config['method'].get('field')
        self.arr4load = []
        # Generate a link for a request
        self.url4requests = urljoin(self.base_url, self.config['add_url'])
        self.insert_limit = 30000

    def pre_execute(self, context: Any):
        self._ch_hook = ClickhouseHook(conn_id=CLICKHOUSE_SCRAT_CONN_ID)
        self.token_api = Connection.get_connection_from_secrets('parcomatica').password

    # pylint: disable=R0912, R0915, R0914
    def execute(self, context: Any):
        self.log.info(f"Process start for {self._table} with method '{self._method}'")
        # Trunc data_interval_start and data_interval_end to beginning of the hour
        start_dt = self._hour_date_trunc(context['data_interval_start'])
        end_dt = self._hour_date_trunc(context['data_interval_end']) - timedelta(seconds=1)
        self.log.info(f"Data will be load from {start_dt} to {end_dt}")
        start_tm, end_tm = int(start_dt.timestamp()), int(end_dt.timestamp())

        # Load data for parking-session
        if self.config['add_url'] == 'parking-session':
            if self._method in ('initial', 'append'):
                self.log.info("Start load data from API")
                self.load_parking_session(start_tm, end_tm)
                if self._method == 'append':
                    # Get a list of id records of open parking sessions
                    self.log.info("Load open parking session from CH")
                    arr_open_ses = self._ch_hook.get_records(
                        f"select distinct id from {self._database}.{self._table} where finishedAt = 0"
                    )
                    self.log.info(f"Found {len(arr_open_ses)} open sessions.")
                    if arr_open_ses:
                        for session_id in arr_open_ses:
                            # Generate a link for a request by record id
                            self.url4requests = urljoin(self.base_url, f"{self.config['add_url']}/{str(session_id[0])}")
                            data = self._response_data_parcomatica()[0]
                            self.arr4load += data
            else:
                raise Exception(f"For parking-session support 'initial' or 'update' load method. Not {self._method}")

        # Load data for parking-session
        elif self.config['add_url'] == 'zone/get-list':
            if self._method in ('initial', 'replace'):
                self.log.info("Getting a list of zones from CH")
                zone_list = self._ch_hook.fetch(
                    "select distinct JSONExtractString(zone, 'number') as zone_number "
                    "from {self._database}.{self._table}"
                )
                self.log.info("Start load data from API")
                data_zone_arr = []
                for zone in zone_list:
                    data_zone_arr.append(self.load_zone(zone))
                # Unpack data for download. The API response is provided in the form [ [], [], [] ]
                self.arr4load = [data_dict for data_list in data_zone_arr for data_dict in data_list]
                del data_zone_arr
                gc.collect()
            else:
                raise Exception(f"For zone support 'initial' or 'replace' load method. Not {self._method}")

            clickhouse.truncate(self._ch_hook, self._database, f"_{self._table}")
            clickhouse.wait_for_mutations(self._ch_hook, self._database, f"_{self._table}")

        # Load data for organization
        elif self.config['add_url'] == 'organization':
            if self._method in ('initial', 'update'):
                self.log.info("Getting a list of organization from CH")
                org_list = self._ch_hook.fetch(
                    "select distinct JSONExtractString(organization, 'id') from {self._database}.{self._table};"
                )
                self.log.info("Start load data from API")
                data_org_arr = []
                for org in org_list:
                    data_org_arr.append(self.load_organization(org))
                # Unpack data for download. The API response is provided in the form [ [], [], [] ]
                self.arr4load = [data_dict for data_list in data_org_arr for data_dict in data_list]
                del data_org_arr
                gc.collect()
                if self._method == 'initial':
                    clickhouse.truncate(self._ch_hook, self._database, f"_{self._table}")
            else:
                raise Exception(f"For organization support 'initial' or 'update' load method. Not {self._method}")

        # Load data for organization
        elif self.config['add_url'] == 'car':
            if self._method == 'replace':
                self.log.info("Start load data from API")
                self.load_car()
                clickhouse.truncate(self._ch_hook, self._database, f"_{self._table}")
            else:
                raise Exception(f"For car support 'replace' load method. Not {self._method}")

        # Load data for user
        elif self.config['add_url'] == 'user/list':
            if self._method == 'replace':
                self.log.info("Start load data from API")
                self.arr4load = self._response_data_parcomatica()[0]
                clickhouse.truncate(self._ch_hook, self._database, f"_{self._table}")
            else:
                raise Exception(f"For user support 'replace' load method. Not {self._method}")

        self.log.info("Delete (if exists) records loaded in the current iteration from the database")
        # pylint: disable=W1401
        arr4delete = [re.findall('"id":\s{0,1}(\d+)', x)[0] for x in self.arr4load]
        self._ch_hook.run(
            f"alter table {self._database}._{self._table} on cluster cluster "
            f"delete where id in ({', '.join(arr4delete)})"
        )
        clickhouse.wait_for_mutations(self._ch_hook, self._database, f"_{self._table}")

        self.log.info(f"Insert data to CH. Will load {len(arr4delete)} records")
        self._insert_json_arr_ch(self.arr4load)

    def load_organization(self, org: str) -> list:
        """
        Method for load organization data from API
        Args:
            org - organization id
        Return:
            List of json
        """
        params = {id: org}
        data_arr = self._response_data_parcomatica(params)[0]
        return data_arr

    def load_car(self):
        """
        Method for loading data on cars.
        When the method is executed, the variable self.arr4load is filled with data
        """
        counter = 1
        page_number = 1
        params = {'page': counter}
        while counter <= page_number:
            data_arr, page_number = self._response_data_parcomatica(params)
            counter += 1
            params.update({'page': counter})
            self.arr4load += data_arr
            time.sleep(0.3)

    def load_zone(self, zone: str) -> list:
        """
        Method for load zone data from API
        Args:
            zone - zone number
        Return:
            List of json
        """
        params = {'number': {zone[0]}}
        data_arr = self._response_data_parcomatica(params)[0]
        return data_arr

    def load_parking_session(self, start_tm: int, end_tm: int):
        """
        Method for loading data on parking sessions.
        When the method is executed, the variable self.arr4load is filled with data
        Args:
            start_tm - date start interval
            end_tm - date end interval
        When the method is executed, the variable self.arr4load is filled with data
        """
        counter = 0
        page_number = 1
        params = {'from': start_tm, 'to': end_tm, 'page': counter}
        if self.status == 'in_progress':
            params.pop('from')
            params.pop('to')
            params['status'] = self.status
        while counter <= page_number - 1:
            data_arr, page_number = self._response_data_parcomatica(params)
            counter += 1
            params.update({'page': counter})
            self.arr4load += data_arr
            time.sleep(0.3)

    # pylint: disable=W0102
    def _response_data_parcomatica(self, params={}) -> tuple[list, int]:
        """
        Method for processing response from API
        Return:
            res_arr - List of json
            page_cnt - Number of pages
        """
        page_cnt = 0
        res_arr = []
        headers = {'Api-Key': self.token_api}
        with requests.Session() as ses:
            with ses.get(self.url4requests, headers=headers, stream=True, params=params) as resp:
                if resp.status_code == 200:
                    data = resp.text
                    if isinstance(json.loads(data), dict):
                        if json.loads(data).get('data'):
                            data_arr = json.loads(data)['data']
                        else:
                            data_arr = json.loads(data)

                        if json.loads(data).get('meta'):
                            data_meta = json.loads(data).get('meta')
                            page_cnt = data_meta['pageCount']

                    elif isinstance(json.loads(data), list):
                        data_arr = json.loads(data)
                    else:
                        raise Exception("Unsupported response from API")

                    if isinstance(data_arr, dict):
                        # Replace characters and translate lists and dictionaries into text format
                        # so that they can be correctly loaded into the database
                        value_dict = {
                            key: str(val).replace("'", '"') if isinstance(val, dict) else val
                            for key, val in data_arr.items()
                        }
                        value_dict = {
                            key: str(val).replace("'", '"') if isinstance(val, list) else val
                            for key, val in value_dict.items()
                        }
                        res_arr.append(json.dumps(value_dict, ensure_ascii=False))

                    else:
                        for data_dict in data_arr:
                            value_dict = {
                                key: str(val).replace("'", '"') if isinstance(val, dict) else val
                                for key, val in data_dict.items()
                            }
                            value_dict = {
                                key: str(val).replace("'", '"') if isinstance(val, list) else val
                                for key, val in value_dict.items()
                            }
                            res_arr.append(json.dumps(value_dict, ensure_ascii=False))
                    if page_cnt:
                        self.log.info(f"Load page {data_meta['currentPage']+1}. All {data_meta['pageCount']} pages")
                else:
                    raise Exception(f"API was return code {resp.status_code}")

        return res_arr, page_cnt

    def _insert_json_arr_ch(self, result_json_arr: list):
        """
        Method for insert data to CH
        Args:
            result_json_arr - list of json
        """
        for batct_number in range(len(result_json_arr) // self.insert_limit + 1):
            batch4load = result_json_arr[batct_number * self.insert_limit : (batct_number + 1) * self.insert_limit]
            self.log.info(f"Loading will be done in {len(result_json_arr) // self.insert_limit + 1} iterations")
            if batch4load:
                self.log.info(f"Insert batch {batct_number + 1} iterations")
                sql_import = f"""
                                INSERT INTO {self._database}.{self._table} FORMAT JSONEachRow {'  '.join(batch4load)};
                                """
                self._ch_hook.run(sql_import)
                clickhouse.wait_replication(self._ch_hook, self._database, self._table)

    @staticmethod
    def _hour_date_trunc(d_time: datetime) -> datetime:
        """
        Method for trunc data_interval_start and data_interval_end to beginning of the hour
        Args:
            d_time - datetime object
        Returns:
            Datetime trunc to beginning of the hour
        """
        return datetime(d_time.year, d_time.month, d_time.day, d_time.hour).replace(tzinfo=LOCAL_TZ)
