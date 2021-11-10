import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from operators import (
        BidBasicInfoToPostgresOperator, BidServiceReqsToPostgresOperator, MarkBidRequestsOperator,
        EvaluateRequestsOperator, AssignSalesRepOperator, UsacQualityCheckOperator
    )


# Python Functions
def log_exec_start(*args, **kwargs):
    """
        log_exec_start function replaces the logic of the dummy operator,
        to log the beginning of the DAG
    """
    current_time = datetime.now()
    logging.info(f"DAG: {kwargs['run_id']} - Execution began at {current_time}")


def log_exec_end(*args, **kwargs):
    """
        log_exec_end function replaces the logic of the dummy operator,
        to log the completion of the DAG
    """
    current_time = datetime.now()
    logging.info(f"DAG: {kwargs['run_id']} - Execution completed at {current_time}")


# DAG Declaration
init_args = {
    'owner': 'leehunte',
    'start_date': datetime(2016,1,2),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes = 5),
    'catchup': False
}
elt_dag = DAG(
        'assign_erate_form-470',
        default_args = init_args,
        description = 'Import USAC eRate 470 applications and assign applicable requests to sales representatives',
        schedule_interval = '@daily',
        max_active_runs = 1,
        tags = ["capstone"]
    )


# DAG Operators
start_operator = PythonOperator(
        dag = elt_dag,
        task_id = 'Begin_Execution',
        python_callable = log_exec_start,
        provide_context = True
    )

import_form470_basic_info = BidBasicInfoToPostgresOperator(
        dag = elt_dag,
        task_id = 'Import_eRate_470s_basic_info',
        db_conn_id = 'localdb',
        http_conn_id = 'usac_opendata'
    )

prelim_eval_requests = MarkBidRequestsOperator(
        dag = elt_dag,
        task_id = 'Mark_eRate_470s_basic_info',
        db_conn_id = 'localdb'
)

import_form470_service_reqs = BidServiceReqsToPostgresOperator(
        dag = elt_dag,
        task_id = 'Import_eRate_470s_service_requests',
        db_conn_id = 'localdb',
        http_conn_id = 'usac_opendata'
    )

appraise_form470_service_reqs = EvaluateRequestsOperator(
        dag = elt_dag,
        task_id = 'Evaluate_eRate_470s_service_requests',
        db_conn_id = 'localdb',
        s3_conn_id = 'dend-capstone',
        bucket_name = 'dhunte-dend-capstone',
        pricing_key = 'product_offerings.xml'
    )

assign_form470_to_salesrep = AssignSalesRepOperator(
        dag = elt_dag,
        task_id = 'Assign_eRate_470s_to_salesrep',
        db_conn_id = 'localdb'
    )

data_quality_check = UsacQualityCheckOperator(
        dag = elt_dag,
        task_id = 'Report_Dag_quality_statistics',
        db_conn_id = 'localdb',
        s3_conn_id = 'dend-capstone',
        bucket_name = 'dhunte-dend-capstone'
    )

end_operator = PythonOperator(
        dag = elt_dag,
        task_id = 'Complete_Execution',
        python_callable = log_exec_end,
        provide_context = True
    )


# DAG Dependencies
start_operator >> import_form470_basic_info >> prelim_eval_requests
prelim_eval_requests >> import_form470_service_reqs >> appraise_form470_service_reqs
appraise_form470_service_reqs >> assign_form470_to_salesrep >> data_quality_check >> end_operator
