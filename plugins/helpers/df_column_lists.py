class ColumnLists:
    form470_all_flds = [
        "application_number","form_nickname","funding_year","ben","f470_status",
        "allowable_contract_date","category_one_description", "category_two_description",
        "rfp_identifier","state_or_local_restrictions","state_or_local_restrictions_description",
        "statewide_state","all_public_schools_districts","all_non_public_schools","all_libraries",
        "created_datetime","created_by","last_modified_datetime","last_modified_by",
        "billed_entity_name","ben_fcc_registration_number","organization_type","organization_status",
        "applicant_type","number_of_eligible_entities","website_url","billed_entity_address1",
        "billed_entity_address2","billed_entity_city","billed_entity_state","billed_entity_zip",
        "billed_entity_zip_ext","latitude","longitude","contact_name","contact_phone",
        "contact_phone_ext","contact_email", "technical_contact_name","technical_contact_title",
        "technical_contact_phone","technical_contact_phone_ext","technical_contact_email",
        "authorized_person_name","authorized_person_phone","authorized_person_phone_ext",
        "authorized_person_email","authorized_person_title","authorized_person_employer","form_version"
    ]

    form470_app_flds = [
        "application_number","form_nickname","funding_year","ben","f470_status",
        "allowable_contract_date","category_one_description", "category_two_description",
        "rfp_identifier","state_or_local_restrictions","state_or_local_restrictions_description",
        "statewide_state","all_public_schools_districts","all_non_public_schools","all_libraries",
        "created_datetime","created_by","last_modified_datetime","last_modified_by"
    ]

    form470_ben_flds = [
        "ben","billed_entity_name","ben_fcc_registration_number","organization_type",
        "organization_status","applicant_type","number_of_eligible_entities","website_url",
        "billed_entity_address1","billed_entity_address2","billed_entity_city",
        "billed_entity_state","billed_entity_zip","billed_entity_zip_ext","latitude","longitude"
    ]

    form470_contacts_flds = [
        "application_number","ben","application_number","ben","contact_name","contact_phone",
        "contact_phone_ext","contact_email", "technical_contact_name","technical_contact_title",
        "technical_contact_phone","technical_contact_phone_ext","technical_contact_email",
        "authorized_person_name","authorized_person_phone","authorized_person_phone_ext",
        "authorized_person_email","authorized_person_title","authorized_person_employer"
    ]

    unmarked_form470_app_flds = ["form470_application_number","category_two_description","has_applicable_requests"]

    mark_form470_app_flds = ["has_applicable_requests","has_applicable_requests","form470_application_number"]

    form470_requests_all_flds = [
        "application_number","funding_year","service_type","function",
        "other_function","manufacturer","other_manufacturer","entities","quantity","unit",
        "minimum_capacity","maximum_capacity","installation_initial_configuration",
        "maintenance_technical_support","form_version"
    ]

    form470_requests_key_flds = ["application_number","service_type","function"]

    form470_requests_flds = [
        "application_number","service_type","function","application_number","funding_year",
        "service_type","function","entities","quantity","unit","manufacturer","minimum_capacity",
        "maximum_capacity","installation_initial_configuration","maintenance_technical_support"
    ]

    form470_requests_eval_flds = [
        "service_request_id","application_number","function","service_type","manufacturer","eligible_entities",
        "quantity","unit","needs_installation","needs_support","estimated_request_value"
    ]

    form470_requests_estimate_form_flds = ["estimated_request_value","application_number"]

    form470_requests_estimate_req_flds = ["estimated_request_value","service_request_id"]

    dataquality_dag_summary_select = [
        "application_number","has_applicable_requests","sales_rep_id","state",
        "service_request_count","application_value","has_applicable_request","request_value"
    ]