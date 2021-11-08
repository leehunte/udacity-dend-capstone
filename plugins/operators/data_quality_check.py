import pandas as pd

from datetime import datetime, timezone, timedelta

from helpers import SqlQueries,ColumnLists

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook


class UsacQualityCheckOperator(BaseOperator):
    ui_color = '#F5F1ED'

    @apply_defaults
    def __init__(
            self,
            s3_conn_id,
            bucket_name,
            db_conn_id,
            db_autocommit = True,
            *args, **kwargs):

        super(UsacQualityCheckOperator, self).__init__(*args, **kwargs)
        self.s3_conn_id = s3_conn_id
        self.bucket_name = bucket_name
        self.db_conn_id = db_conn_id
        if db_autocommit is not None:
            self.db_autocommit = db_autocommit

    def execute(self, context):
        self.log.info(f"DAG: {context['run_id']} - UsacQualityCheckOperator executing")

        # Retrieve the date before the Execution Date
        day_before = datetime.strftime(datetime.strptime(context['ds'], '%Y-%m-%d') - timedelta(days = 1), '%Y-%m-%d')

        # Create the AWS Redshift connection via the PostgresHook
        aws_rds_hook = PostgresHook(self.db_conn_id)

        # Create the AWS S3 connection via the S3Hook
        s3_hook = S3Hook(self.s3_conn_id)

        date_list = list([f"{day_before}T00:00:00", f"{day_before}T23:59:59"])
        hook_data = aws_rds_hook.get_records(SqlQueries.dataquality_dag_summary_select, date_list)
        df_summary = pd.DataFrame(hook_data, columns = ColumnLists.dataquality_dag_summary_select)

        # Pull statistics for our dict object
        import_total = df_summary['application_number'].nunique()
        assignments = df_summary.assign(flagged_reqs = lambda t: (t['service_request_count'] > 0))[['application_number','has_applicable_requests','flagged_reqs']].drop_duplicates().groupby(['has_applicable_requests','flagged_reqs']).nunique()
        total_value = df_summary['request_value'].agg(sum)
        state_counts = df_summary[['state','request_value']].sort_values('state').groupby('state')['request_value'].agg(sum)
        rep_counts = df_summary[['sales_rep_id','request_value']].sort_values('sales_rep_id').groupby('sales_rep_id')['request_value'].agg(sum)

        # report template
        report = {
            "metadata": {
                "dag_run_id": context['dag_run'].run_id,
                "import_date": day_before,
                "run_start_datetime": context['dag_run'].start_date.strftime('%Y-%m-%dT%H:%M:%S'),
                "run_duration": str(datetime.now(timezone.utc) - context['dag_run'].start_date)
            },
            "statistics": {
                "applications": {
                    "count": import_total,
                    "eligible": {
                        "total": sum(assignments['application_number'][True]),
                        "false_positive": int(assignments['application_number'][True][False])
                    },
                    "irrelevant": {
                        "total": int(assignments['application_number'][False][False])
                    }
                },
                "monetary": {
                    "total_value": total_value,
                    "states": [],
                    "sales_reps": []
                }
            }
        }

        for state_name, dollar_decimal in state_counts.iteritems():
            report['statistics']['monetary']['states'].append({ "name": state_name, "value": str(dollar_decimal) })

        for sales_rep, dollar_decimal in rep_counts.iteritems():
            report['statistics']['monetary']['sales_reps'].append({ "rep_id": sales_rep, "value": str(dollar_decimal) })

        # Upload the report to the S3 bucket, with the internally designed name
        s3_hook.load_string(str(report), f"erate_form-470_logs/{context['dag_run'].run_id}.log", bucket_name=self.bucket_name, replace=True)
