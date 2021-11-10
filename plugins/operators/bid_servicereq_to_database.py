import json
import numpy as np
import pandas as pd

from helpers import SqlQueries,ColumnLists,ServiceSelector

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.http_hook import HttpHook
from airflow.hooks.postgres_hook import PostgresHook


class BidServiceReqsToPostgresOperator(BaseOperator):
    ui_color = '#74A57F'

    @apply_defaults
    def __init__(
            self,
            db_conn_id,
            http_conn_id,
            db_autocommit = True,
            *args, **kwargs):

        super(BidServiceReqsToPostgresOperator, self).__init__(*args, **kwargs)
        self.db_conn_id = db_conn_id
        self.http_conn_id = http_conn_id
        if db_autocommit is not None:
            self.db_autocommit = db_autocommit

    def execute(self, context):
        self.log.info(f"DAG: {context['run_id']} - BidServiceReqsToPostgresOperator executing")

        # Create the AWS Redshift connection via the PostgresHook
        aws_rds_hook = PostgresHook(self.db_conn_id)

        # SELECT high-level applications that have been flagged
        fetched_reqs = aws_rds_hook.get_records(SqlQueries.usac_erate_470forms_uncounted_select)
        self.log.info(f"DAG: {context['run_id']} - Processing {len(fetched_reqs)} records")

        # Create the HTTP client to request data
        http_client = HttpHook(http_conn_id = self.http_conn_id, method = 'GET')

        req_count = 0
        for req in fetched_reqs:
            # Run HTTP GET request and convert returned data into a Pandas Data.Frame
            url = "/resource/39tn-hjzv.json?application_number={}&service_category=Category 2"
            services = json.loads((http_client.run(endpoint = url.format(req[0]))).text)

            df_apps = pd.DataFrame(services) \
                .sort_values(by = ["application_number","form_version"], ascending = [True,False]) \
                .reindex(columns = ColumnLists.form470_requests_all_flds) \
                .replace({np.nan: None}) \
                .drop_duplicates(ColumnLists.form470_requests_key_flds, keep = 'last')
            req_count += len(df_apps)

            # Identify whether 
            df_apps['manufacturer'] = df_apps[['manufacturer','other_manufacturer']].apply(ServiceSelector.replace_with_other_value, axis = 1)
            df_apps['function'] = df_apps[['function','other_function']].apply(ServiceSelector.replace_with_other_value, axis = 1)

            # INSERT the high-level application records
            for ix, row in df_apps[ColumnLists.form470_requests_flds].iterrows():
                aws_rds_hook.run(SqlQueries.usac_erate_470forms_reqs_upsert, self.db_autocommit, parameters = list(row))

        self.log.info(f"DAG: {context['run_id']} - INSERT {req_count} records into erate_470form_requests table")
