import logging

import requests
from airflow.exceptions import AirflowFailException
from airflow.hooks.base_hook import BaseHook

logger = logging.getLogger('utils_logger')


class ParametizedHttpHook(BaseHook):
    def __init__(self, http_conn_id, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.http_conn_id = http_conn_id

    def get_conn(self, headers=None):
        conn = self.get_connection(self.http_conn_id)
        return conn.host

    def run(self, endpoint, data=None, params=None):
        """
        Run HTTP request.

        :param endpoint: The relative endpoint URL (e.g., '/api/v1/resource')
        :param data: The data to be sent in the request (if any)
        :param params: Additional params to include in the request
        """
        try:
            host = self.get_conn()
            url = f'{host}{endpoint}'
            response = requests.get(url, data=data, params=params)
        except requests.exceptions.HTTPError as e:
            raise AirflowFailException(e)

        if response.status_code != 200:
            self.log.error('HTTP request failed: %s', response.text)
            response.raise_for_status()

        return response
