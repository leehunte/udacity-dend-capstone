from datetime import datetime, timedelta

from helpers import SqlQueries

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.postgres_hook import PostgresHook


class AssignSalesRepOperator(BaseOperator):
    ui_color = '#E4C5AF'

    @apply_defaults
    def __init__(
            self,
            db_conn_id,
            db_autocommit = True,
            *args, **kwargs):

        super(AssignSalesRepOperator, self).__init__(*args, **kwargs)
        self.db_conn_id = db_conn_id
        if db_autocommit is not None:
            self.db_autocommit = db_autocommit

    def execute(self, context):
        self.log.info(f"DAG: {context['run_id']} - AssignSalesRepOperator executing")

        # Create the AWS Redshift connection via the PostgresHook
        aws_rds_hook = PostgresHook(self.db_conn_id)

        # SELECT high-level applications that have been flagged
        day_before = datetime.strftime(datetime.strptime(context['ds'], '%Y-%m-%d') - timedelta(days = 1), '%Y-%m-%d')
        self.log.info(f"DAG: {context['run_id']} - Execution Date: {context['ds']}")
        self.log.info(f"DAG: {context['run_id']} - Application Date: {day_before}")

        date_list = list([f"{day_before}T00:00:00", f"{day_before}T23:59:59"])
        fetched_unassigned = aws_rds_hook.get_records(SqlQueries.usac_erate_470forms_unassigned_select, date_list)
        self.log.info(f"DAG: {context['run_id']} - SELECT {len(fetched_unassigned)} rows from billing_entities table")

        for assign in fetched_unassigned:
            # Run a query to determine the appropriate sales rep for the sales opportunity
            sales_rep_id = aws_rds_hook.get_records(SqlQueries.business_match_rep_erate_470forms_select, list([assign[2], assign[3]]))

            try:
                # Record the sales opportunity assignment into the sales_erate_470forms_assignment table
                aws_rds_hook.run(SqlQueries.usac_erate_470forms_assign_sales_rep_upsert, self.db_autocommit, parameters = list([assign[0], sales_rep_id[0], assign[1], assign[2], assign[3]]))
            except IndexError:
                self.log.error(f"DAG: {context['run_id']} - sales_rep_id variable: {sales_rep_id}")
                self.log.error(f"DAG: {context['run_id']} - assign variable: {assign}")
                aws_rds_hook.run(SqlQueries.usac_erate_470forms_assign_sales_rep_upsert, self.db_autocommit, parameters = list([assign[0], '00-000-0000', assign[1], assign[2], assign[3]]))