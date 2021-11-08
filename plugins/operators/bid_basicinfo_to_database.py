import json
import numpy as np
import pandas as pd

from datetime import datetime, timedelta

from helpers import SqlQueries,ColumnLists

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.http_hook import HttpHook
from airflow.hooks.postgres_hook import PostgresHook


class BidBasicInfoToPostgresOperator(BaseOperator):
    ui_color = '#074F57'

    @apply_defaults
    def __init__(
            self,
            db_conn_id,
            http_conn_id,
            db_autocommit = True,
            *args, **kwargs):

        super(BidBasicInfoToPostgresOperator, self).__init__(*args, **kwargs)
        self.db_conn_id = db_conn_id
        self.http_conn_id = http_conn_id
        if db_autocommit is not None:
            self.db_autocommit = db_autocommit

    def execute(self, context):
        self.log.info(f"DAG: {context['run_id']} - BidBasicInfoToPostgresOperator executing")

        # most 470 forms will be submitted during buisnes hours; so imports will be from the previous day of the execution_date
        day_before = datetime.strftime(datetime.strptime(context['ds'], '%Y-%m-%d') - timedelta(days = 1), '%Y-%m-%d')
        self.log.info(f"DAG: {context['run_id']} - Execution Date: {context['ds']}")
        self.log.info(f"DAG: {context['run_id']} - Application Date: {day_before}")

        # apply ds keyword argument to URL querystring to create URL request
        url = f"resource/jp7a-89nd.json?$where=last_modified_datetime between '{day_before}T00:00:00' and '{day_before}T23:59:59'"

        # Create the HTTP client to request data
        http_client = HttpHook(http_conn_id = self.http_conn_id, method = 'GET')

        # Run HTTP GET request and convert returned data into a Pandas Data.Frame
        applications = json.loads((http_client.run(endpoint = url)).text)
        self.log.info(f"DAG: {context['run_id']} - {len(applications)} eRate 470 form applications retrieved")

        if len(applications) > 0:
            df_apps = pd.DataFrame(applications) \
                .sort_values(by = ["application_number","form_version","last_modified_by"], ascending = [True,False,True]) \
                .reindex(columns = ColumnLists.form470_all_flds) \
                .replace({np.nan: None}) \
                .drop_duplicates(["application_number"], keep = 'last')
            self.log.info(f"DAG: {context['run_id']} - {len(df_apps)} eRate 470 form applications to be inserted")

            # Create the AWS Redshift connection via the PostgresHook
            aws_rds_hook = PostgresHook(self.db_conn_id)

            # INSERT the high-level application records
            for ix, row in df_apps[ColumnLists.form470_app_flds].iterrows():
                aws_rds_hook.run(SqlQueries.usac_erate_470forms_upsert, self.db_autocommit, parameters = list(row))

            # INSERT the billing entity records
            for idx, row in df_apps.drop_duplicates(["ben"], keep = 'last')[ColumnLists.form470_ben_flds].iterrows():
                aws_rds_hook.run(SqlQueries.billing_entities_insert, self.db_autocommit, parameters = list(row))

            # INSERT the application contacts records
            for idx, row in df_apps[ColumnLists.form470_contacts_flds].iterrows():
                aws_rds_hook.run(SqlQueries.usac_erate_470form_requesters_upsert, self.db_autocommit, parameters = list(row))
