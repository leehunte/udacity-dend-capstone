import pandas as pd
import xml.etree.ElementTree as et

from helpers import SqlQueries,ColumnLists,ServiceRequestAppraiser

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook

### TODO: Configure Operator to take an AWS S3 Connection and import the pricing XML
class EvaluateRequestsOperator(BaseOperator):
    ui_color = '#9ECE9A'
    offerings = None

    @apply_defaults
    def __init__(
            self,
            s3_conn_id,
            bucket_name,
            pricing_key,
            db_conn_id,
            db_autocommit = True,
            *args, **kwargs):

        super(EvaluateRequestsOperator, self).__init__(*args, **kwargs)
        self.db_conn_id = db_conn_id
        if db_autocommit is not None:
            self.db_autocommit = db_autocommit

        # Connect to AWS S3 to retrieve the product pricing XML
        s3_hook = S3Hook(s3_conn_id)
        text_data = s3_hook.read_key(bucket_name=bucket_name, key=pricing_key)
        self.offerings = et.fromstring(text_data)

    def __GetPriceNode(self, funct, manuf):
        """
        Given a Service Function and Manufacturer name, search the pricing XML for an appropriate pricing node
        """
        manuf = manuf[7:] if manuf.startswith("Other: ") else manuf

        try:
            product = self.offerings.find(f"function[@name='{funct}']/dictionary/pair[@option='{manuf}']").attrib["product"]
        except AttributeError as ae:
            product = self.offerings.find(f"function[@name='{funct}']/dictionary").attrib["default"]

        costs = self.offerings.find(f"function[@name='{funct}']/products/product[@name='{product}']")

        return costs

    def execute(self, context):
        self.log.info(f"DAG: {context['run_id']} - EvaluateRequestsOperator executing")

        # Create a ServiceRequestAppraiser instance
        appraiser = ServiceRequestAppraiser()

        # Create the AWS Redshift connection via the PostgresHook
        aws_rds_hook = PostgresHook(self.db_conn_id)

        # SELECT high-level applications that have been flagged
        fetched_reqs = aws_rds_hook.get_records(SqlQueries.usac_erate_470forms_reqs_valueless)
        df_reqs = pd.DataFrame(fetched_reqs, columns = ColumnLists.form470_requests_eval_flds)

        # Loop through all the records in the df_reqs DataFrame
        for idx, req in df_reqs.iterrows():
            fn = req["function"]
            vendor = req["manufacturer"]
            # Retrieve the applicable product pricing XML node
            price_node = self.__GetPriceNode(fn, vendor)

            svctype = req["service_type"]
            # estimate a dollar value for the service request, based on the function and service type
            req["estimated_request_value"] = appraiser.matrix[fn][svctype](req, price_node)

            # Update the erate_470form_requests and erate_470forms tables to reflect the dollar values estimated
            aws_rds_hook.run(SqlQueries.usac_erate_470forms_reqs_value_update, self.db_autocommit, parameters = list(req[ColumnLists.form470_requests_estimate_req_flds]))
            aws_rds_hook.run(SqlQueries.usac_erate_470forms_value_update, self.db_autocommit, parameters = list(req[ColumnLists.form470_requests_estimate_form_flds]))
