from __future__ import division, absolute_import, print_function

from airflow.plugins_manager import AirflowPlugin

import operators
import helpers

# Defining the plugin class
class UdacityPlugin(AirflowPlugin):
    name = "udacity_plugin"
    operators = [
        operators.BidBasicInfoToPostgresOperator,
        operators.BidServiceReqsToPostgresOperator,
        operators.MarkBidRequestsOperator,
        operators.EvaluateRequestsOperator,
        operators.AssignSalesRepOperator,
        operators.UsacQualityCheckOperator
    ]
    helpers = [
        helpers.SqlQueries,
        helpers.ColumnLists,
        helpers.ServiceSelector,
        helpers.ServiceRequestAppraiser
    ]
