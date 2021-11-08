import pandas as pd

from helpers import SqlQueries,ColumnLists,ServiceSelector

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.postgres_hook import PostgresHook


class MarkBidRequestsOperator(BaseOperator):
    ui_color = '#077187'

    @apply_defaults
    def __init__(
            self,
            db_conn_id,
            db_autocommit = True,
            *args, **kwargs):

        super(MarkBidRequestsOperator, self).__init__(*args, **kwargs)
        self.db_conn_id = db_conn_id
        if db_autocommit is not None:
            self.db_autocommit = db_autocommit

    def execute(self, context):
        self.log.info(f"DAG: {context['run_id']} - MarkBidRequestsOperator executing")

        # Create the AWS Redshift connection via the PostgresHook
        aws_rds_hook = PostgresHook(self.db_conn_id)

        # SELECT high-level applications that have not been flagged yet
        fetched_reqs = aws_rds_hook.get_records(SqlQueries.usac_erate_470forms_unmarked_select)
        unmarked = pd.DataFrame(fetched_reqs, columns = ColumnLists.unmarked_form470_app_flds)

        if len(unmarked) > 0:
            # Run a word search on the category_two_description field to verify if the service request includes WAP
            unmarked['has_applicable_requests'] = unmarked['category_two_description'].apply(ServiceSelector.is_applicable_request)

            # Create a summary table, grouped by has_applicable_requests field, to log the ratio of applicable requests
            mark_summary = unmarked.groupby("has_applicable_requests").size()
            self.log.info(f"DAG: {context['run_id']} - INSERT {mark_summary[True]} APPLICABLE rows into usac_erate_470forms table")
            self.log.info(f"DAG: {context['run_id']} - INSERT {mark_summary[False]} IRRELEVANT rows into usac_erate_470forms table")

            # UPDATE high-level applications 
            for idx, row in unmarked[ColumnLists.mark_form470_app_flds].iterrows():
                aws_rds_hook.run(SqlQueries.usac_erate_470forms_mark_update, self.db_autocommit, parameters = list(row))
