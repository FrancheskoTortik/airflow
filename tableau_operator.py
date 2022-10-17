# -*- coding: utf-8 -*-

import pendulum
import requests
import urllib.parse
import time
from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator
from airflow.models import Variable


class TableauRefreshOperator(BaseOperator):
    """
    Триггерит обновление воркбука, используя метод refresh из Tableau REST API.
    https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref.htm#update_workbook_now
    """

    def __init__(self, workbook, refresh_interval, version='3.7', tableau_conn_id='tableau',
                 *args, **kwargs):

        super(TableauRefreshOperator, self).__init__(*args, **kwargs)
        self.version = version
        self.workbook = workbook
        self.refresh_interval = refresh_interval
        self.tableau_conn_id = tableau_conn_id

    def execute(self, context):
        tableau_conn = BaseHook.get_connection(self.tableau_conn_id)
        self.api_url = tableau_conn.host + f'/api/{self.version}'
        self.headers = {'Content-Type': 'application/json', 'Accept': 'application/json'}

        site_id, auth_token = self.get_auth(tableau_conn.login, tableau_conn.password)

        self.headers_with_auth = {**self.headers, 'X-Tableau-Auth': auth_token}
        self.site_url = self.api_url + f'/sites/{site_id}'

        if self.previous_job_is_running():
            raise AirflowSkipException('Previously started job is still running. Exiting...')

        workbook_info = self.get_workbook_info(self.workbook)

        last_update_utc = pendulum.parse(workbook_info['updatedAt'])
        while True:
            not_updated_for = pendulum.utcnow() - last_update_utc
            if not_updated_for < (2 / 3) * self.refresh_interval:
                raise AirflowSkipException(
                    f'Workbook was last updated at {last_update_utc.astimezone(Variable.get("timezone")).to_iso8601_string()}. '
                    f'Too early to update it again, skipping task...')
            elif not_updated_for < self.refresh_interval:
                sleep_delay = (self.refresh_interval / 12).seconds
                self.log.info(f'Waiting to run refresh. Sleeping for {sleep_delay} sec...')
                time.sleep(sleep_delay)
                continue
            else:
                break

        job_id = self.run_refresh_job(workbook_info['id'])
        self.monitor_job(job_id)

    def get_auth(self, token_name, token_secret):
        auth_url = self.api_url + '/auth/signin'
        data = {
            'credentials': {
                'personalAccessTokenName': token_name,
                'personalAccessTokenSecret': token_secret,
                'site': {'contentUrl': ''}
            }
        }

        try:
            resp = requests.post(auth_url, json=data, headers=self.headers)
            json_resp = resp.json()

            site_id = json_resp['credentials']['site']['id']
            auth_token = json_resp['credentials']['token']
        except KeyError:
            self.log.exception(json_resp)
            raise
        return site_id, auth_token

    def get_workbook_info(self, name):
        workbooks_url = self.site_url + f'/workbooks?filter=name:eq:{name}'
        r = requests.get(workbooks_url, headers=self.headers_with_auth)
        json_resp = r.json()

        workbooks = json_resp['workbooks']['workbook']
        if len(workbooks) != 1:
            raise AirflowException(f'Query should return one and only one workbook, but returned {len(workbooks)}.')

        return {'id': workbooks[0]['id'], 'updatedAt': workbooks[0]['updatedAt']}

    def previous_job_is_running(self):
        filters = [
            f'jobType:eq:refresh_extracts',
            f'title:has:{urllib.parse.quote_plus(self.workbook)}',
            f'status:eq:InProgress'
        ]
        jobs_url = self.site_url + f'/jobs?filter={",".join(filters)}'
        try:
            r = requests.get(jobs_url, headers=self.headers_with_auth)
            json_resp = r.json()
            return len(json_resp['backgroundJobs']['backgroundJob']) > 0
        except KeyError:
            self.log.exception(json_resp)
            raise

    def get_job(self, job_id):
        job_url = self.site_url + f'/jobs/{job_id}'
        try:
            r = requests.get(job_url, json={}, headers=self.headers_with_auth)
            json_resp = r.json()
            job_info = json_resp['job']
        except KeyError:
            self.log.exception(json_resp)
            raise
        return job_info

    def run_refresh_job(self, workbook_id):
        refresh_url = self.site_url + f"/workbooks/{workbook_id}/refresh"
        try:
            r = requests.post(refresh_url, json={}, headers=self.headers_with_auth)
            json_resp = r.json()
            job_id = json_resp['job']['id']
            self.log.info(f'job_id: {job_id}')
        except KeyError:
            raise AirflowException(json_resp)
        return job_id

    def monitor_job(self, job_id):
        while True:
            job_info = self.get_job(job_id)
            sleep_delay = 30  # seconds

            if 'finishCode' not in job_info:
                self.log.info(f'Waiting for job to complete. Sleeping for {sleep_delay} sec...')
                time.sleep(sleep_delay)
                continue

            exit_code = job_info['finishCode']
            progress = job_info['progress']

            if exit_code == '0':
                self.log.info('Job is complete successfully')
                break
            elif exit_code == '1' and progress != '100':
                self.log.info(f'Job progress: {progress}%')
                time.sleep(sleep_delay)
                continue
            else:
                raise Exception(f'Job is complete with errors', job_info)
