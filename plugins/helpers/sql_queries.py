class SqlQueries:
    usac_erate_470forms_upsert = ("""INSERT INTO usac.erate_470forms AS DEST (
    application_number, nickname, funding_year, billing_entity_number,
    fcc_form_status, allowable_contract_date, category_one_description,
    category_two_description, rfp_id, government_restrictions,
    government_restriction_descriptions, is_statewide, is_statewide_public_schools,
    is_statewide_nonpublic_schools, is_statewide_libraries, creation_date,
    created_by, modified_date, modified_by
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (application_number)
    DO UPDATE
    SET
        allowable_contract_date = EXCLUDED.allowable_contract_date,
        category_one_description = EXCLUDED.category_one_description,
        category_two_description = EXCLUDED.category_two_description,
        rfp_id = EXCLUDED.rfp_id,
        government_restrictions = EXCLUDED.government_restrictions,
        government_restriction_descriptions = EXCLUDED.government_restriction_descriptions,
        is_statewide = EXCLUDED.is_statewide,
        is_statewide_public_schools = EXCLUDED.is_statewide_public_schools,
        is_statewide_nonpublic_schools = EXCLUDED.is_statewide_nonpublic_schools,
        is_statewide_libraries = EXCLUDED.is_statewide_libraries,
        modified_date = EXCLUDED.modified_date,
        modified_by = EXCLUDED.modified_by,
        has_applicable_requests = NULL
    WHERE DEST.application_number = EXCLUDED.application_number;""")

    billing_entities_insert = ("""INSERT INTO usac.billing_entities AS DEST (
    entity_number, entity_name, fcc_registration_number, organization_type,
    organization_status, applicant_type, eligible_entities, website_url, address_line_1,
    address_line_2, city, state, zip_code, zip_code_extnsion, latitude, longitude
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (entity_number) DO NOTHING;""")

    usac_erate_470form_requesters_upsert = ("""INSERT INTO usac.erate_470form_requesters AS DEST (
    application_request_uid, application_number, billing_entity_number,
    contact_name, contact_phone, contact_phone_extension, contact_email,
    technical_name, technical_title, technical_phone, technical_phone_extension,
    technical_email, authority_name, authority_phone, authority_phone_extension,
    authority_title, authority_email, authority_employer
)
VALUES (md5(%s || %s), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (application_number, billing_entity_number)
    DO UPDATE
    SET
        contact_name = EXCLUDED.contact_name,
        contact_phone = EXCLUDED.contact_phone,
        contact_phone_extension = EXCLUDED.contact_phone_extension,
        contact_email = EXCLUDED.contact_email,
        technical_name = EXCLUDED.technical_name,
        technical_title = EXCLUDED.technical_title,
        technical_phone = EXCLUDED.technical_phone,
        technical_phone_extension = EXCLUDED.technical_phone_extension,
        technical_email = EXCLUDED.technical_email,
        authority_name = EXCLUDED.authority_name,
        authority_phone = EXCLUDED.authority_phone,
        authority_phone_extension = EXCLUDED.authority_phone_extension,
        authority_title = EXCLUDED.authority_title,
        authority_email = EXCLUDED.authority_email,
        authority_employer = EXCLUDED.authority_employer
    WHERE DEST.application_number = EXCLUDED.application_number
        AND DEST.billing_entity_number = EXCLUDED.billing_entity_number;""")

    usac_erate_470forms_unmarked_select = ("""SELECT application_number, category_two_description, has_applicable_requests
FROM usac.erate_470forms
WHERE has_applicable_requests IS NULL;""")

    usac_erate_470forms_mark_update = ("""UPDATE usac.erate_470forms
SET
    has_applicable_requests = %s,
    service_request_count = CASE WHEN %s THEN 0 ELSE -1 END
WHERE application_number = %s;""")


    usac_erate_470forms_uncounted_select = ("""SELECT application_number
FROM usac.erate_470forms
WHERE has_applicable_requests = True
    AND service_request_count = 0;""")

    usac_erate_470forms_reqs_upsert = ("""INSERT INTO usac.erate_470form_requests AS DEST (
    service_request_id, application_number, funding_year, service_type,
    function, applicable_entities, quantity, unit, manufacturer,
    min_capacity, max_capacity, needs_installation, needs_support
) VALUES(md5(%s || %s || %s), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (service_request_id)
    DO UPDATE
    SET
        applicable_entities = EXCLUDED.applicable_entities,
        quantity = EXCLUDED.quantity,
        unit = EXCLUDED.unit,
        manufacturer = EXCLUDED.manufacturer,
        min_capacity = EXCLUDED.min_capacity,
        max_capacity = EXCLUDED.max_capacity,
        needs_installation = EXCLUDED.needs_installation,
        needs_support = EXCLUDED.needs_support,
        estimated_dollar_value = 0.00
    WHERE DEST.application_number = EXCLUDED.application_number
        AND DEST.service_type = EXCLUDED.service_type
        AND DEST.function = EXCLUDED.function;""")

    usac_erate_470forms_reqs_valueless = ("""SELECT r.service_request_id, f.application_number, r.function, r.service_type, r.manufacturer,
    COALESCE(r.applicable_entities, e.eligible_entities) AS eligible_entities, COALESCE(r.quantity, 0) AS quantity,
    r.unit, r.needs_installation, r.needs_support, r.estimated_dollar_value AS estimated_request_value
FROM usac.erate_470forms AS f
    INNER JOIN usac.erate_470form_requests AS r ON f.application_number = r.application_number
    INNER JOIN usac.billing_entities AS e ON f.billing_entity_number = e.entity_number
WHERE f.has_applicable_requests = TRUE
    AND f.service_request_count = 0
    AND r.function IN ('Cabling','Racks','Switches','Wap','Wireless Controller')
    AND (
        SELECT COUNT ('x')
        FROM usac.erate_470form_requests
        WHERE application_number = f.application_number
            AND function IN ('Wap', 'Wireless Controller')
            AND service_type = 'Internal Connections'
    ) > 0
ORDER BY f.application_number ASC;""")

    usac_erate_470forms_reqs_value_update = ("""UPDATE usac.erate_470form_requests
SET
    estimated_dollar_value = %s,
    has_applicable_request = TRUE
WHERE service_request_id = %s;""")

    usac_erate_470forms_value_update = ("""UPDATE usac.erate_470forms
SET
    estimated_dollar_value = estimated_dollar_value + %s,
    service_request_count = service_request_count + 1
WHERE application_number = %s;""")

    usac_erate_470forms_unassigned_select = ("""SELECT f.application_number, f.billing_entity_number, e.state, f.estimated_dollar_value
FROM usac.erate_470forms AS f
    INNER JOIN usac.billing_entities AS e ON f.billing_entity_number = e.entity_number
WHERE f.has_applicable_requests = TRUE
    AND f.modified_date BETWEEN %s AND %s
    AND f.estimated_dollar_value > 0;""")

    business_match_rep_erate_470forms_select = ("""SELECT sales_rep_id
FROM business.sales_state_assignment
WHERE state_abbreviation = %s
    AND %s BETWEEN low_job_value AND hight_job_value;""")

    usac_erate_470forms_assign_sales_rep_upsert = ("""INSERT INTO business.sales_erate_470forms_assignment AS DEST (
    application_number, sales_rep_id, billing_entity_number, state, estimated_dollar_value
) VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (application_number)
    DO UPDATE
    SET
        sales_rep_id = EXCLUDED.sales_rep_id,
        estimated_dollar_value = EXCLUDED.estimated_dollar_value
    WHERE DEST.application_number = EXCLUDED.application_number;""")

    dataquality_dag_summary_select = ("""SELECT f.application_number, f.has_applicable_requests, a.sales_rep_id, a.state,
    f.service_request_count, COALESCE(f.estimated_dollar_value, 0.00) AS application_value,
    r.has_applicable_request, COALESCE(r.estimated_dollar_value, 0.00) AS request_value
FROM usac.erate_470forms AS f
    LEFT OUTER JOIN usac.erate_470form_requests AS r ON f.application_number = r.application_number
    LEFT OUTER JOIN business.sales_erate_470forms_assignment AS a ON f.application_number = a.application_number
WHERE f.modified_date BETWEEN %s AND %s
ORDER BY f.application_number ASC;""")
