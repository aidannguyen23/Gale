import pandas as pd
import os
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)  # Parent directory (root)
DATA_PATH = os.path.join(PROJECT_ROOT, "data", "PERM Program")

# ---------------------------------------------------------
# FINAL SCHEMA
# ---------------------------------------------------------
FINAL_SCHEMA = [
    "CASE_NUMBER", "CASE_STATUS", "RECEIVED_DATE", "DECISION_DATE", "OCCUPATION_TYPE",
    "EMP_BUSINESS_NAME", "EMP_TRADE_NAME", "EMP_ADDR1", "EMP_ADDR2", "EMP_CITY", 
    "EMP_STATE", "EMP_POSTCODE", "EMP_COUNTRY", "EMP_PROVINCE", "EMP_PHONE", 
    "EMP_PHONEEXT", "EMP_FEIN", "EMP_NAICS", "EMP_NUM_PAYROLL", "EMP_YEAR_COMMENCED",
    "EMP_WORKER_INTEREST", "EMP_RELATIONSHIP_WORKER",
    "EMP_POC_LAST_NAME", "EMP_POC_FIRST_NAME", "EMP_POC_MIDDLE_NAME", "EMP_POC_JOB_TITLE",
    "EMP_POC_ADDR1", "EMP_POC_ADDR2", "EMP_POC_CITY", "EMP_POC_STATE", 
    "EMP_POC_POSTAL_CODE", "EMP_POC_COUNTRY", "EMP_POC_PROVINCE", "EMP_POC_PHONE",
    "EMP_POC_PHONEEXT", "EMP_POC_EMAIL",
    "ATTY_AG_REP_TYPE", "ATTY_AG_LAST_NAME", "ATTY_AG_FIRST_NAME", "ATTY_AG_MIDDLE_NAME",
    "ATTY_AG_ADDRESS1", "ATTY_AG_ADDRESS2", "ATTY_AG_CITY", "ATTY_AG_STATE",
    "ATTY_AG_POSTAL_CODE", "ATTY_AG_COUNTRY", "ATTY_AG_PROVINCE", "ATTY_AG_PHONE",
    "ATTY_AG_PHONE_EXT", "ATTY_AG_EMAIL", "ATTY_AG_LAW_FIRM_NAME", "ATTY_AG_FEIN",
    "ATTY_AG_STATE_BAR_NUMBER", "ATTY_AG_GOOD_STANDING_STATE", "ATTY_AG_GOOD_STANDING_COURT",
    "FW_INFO_APPX_A_ATTACHED", "FW_INFO_ATTY_OR_AGENT",
    "JOB_OPP_PWD_NUMBER", "JOB_TITLE", "JOB_OPP_PWD_ATTACHED", "JOB_OPP_WAGE_FROM",
    "JOB_OPP_WAGE_TO", "JOB_OPP_WAGE_PER", "JOB_OPP_WAGE_CONDITIONS",
    "PRIMARY_WORKSITE_TYPE", "PRIMARY_WORKSITE_ADDR1", "PRIMARY_WORKSITE_ADDR2",
    "PRIMARY_WORKSITE_CITY", "PRIMARY_WORKSITE_COUNTY", "PRIMARY_WORKSITE_STATE",
    "PRIMARY_WORKSITE_POSTAL_CODE", "PRIMARY_WORKSITE_BLS_AREA", "IS_MULTIPLE_LOCATIONS",
    "IS_APPENDIX_B_ATTACHED",
    "OTHER_REQ_IS_FULLTIME_EMP", "OTHER_REQ_IS_LIVEIN_HOUSEHOLD", "OTHER_REQ_IS_PAID_EXPERIENCE",
    "OTHER_REQ_IS_FW_EXECUTED_CONT", "OTHER_REQ_IS_EMP_PROVIDED_CONT", "OTHER_REQ_ACCEPT_DIPLOMA_PWD",
    "OTHER_REQ_IS_FW_CURRENTLY_WRK", "OTHER_REQ_IS_FW_QUALIFY", "OTHER_REQ_EMP_WILL_ACCEPT",
    "OTHER_REQ_EMP_RELY_EXP", "OTHER_REQ_FW_GAIN_EXP", "OTHER_REQ_EMP_PAY_EDUCATION",
    "OTHER_REQ_JOB_EMP_PREMISES", "OTHER_REQ_JOB_COMBO_OCCUP", "OTHER_REQ_JOB_FOREIGN_LANGUAGE",
    "OTHER_REQ_JOB_REQ_EXCEED", "OTHER_REQ_EMP_USE_CREDENTIAL", "OTHER_REQ_EMP_REC_PAYMENT",
    "OTHER_REQ_EMP_LAYOFF",
    "RECR_INFO_RECRUIT_SUPERVISED_REQ", "RECR_INFO_JOB_START_DATE", "RECR_INFO_JOB_END_DATE",
    "RECR_INFO_IS_NEWSPAPER_SUNDAY", "RECR_INFO_NEWSPAPER_NAME", "RECR_INFO_AD_DATE1",
    "RECR_INFO_RECRUIT_AD_TYPE", "RECR_INFO_NEWSPAPER_NAME2", "RECR_INFO_AD_DATE2",
    "RECR_OCC_JOB_FAIR_FROM", "RECR_OCC_JOB_FAIR_TO", "RECR_OCC_EMP_WEBSITE_FROM",
    "RECR_OCC_EMP_WEBSITE_TO", "RECR_OCC_JOB_SEARCH_FROM", "RECR_OCC_JOB_SEARCH_TO",
    "RECR_OCC_ON_CAMPUS_FROM", "RECR_OCC_ON_CAMPUS_TO", "RECR_OCC_TRADE_ORG_FROM",
    "RECR_OCC_TRADE_ORG_TO", "RECR_OCC_PRIVATE_EMP_FROM", "RECR_OCC_PRIVATE_EMP_TO",
    "RECR_OCC_EMP_REFERRAL_FROM", "RECR_OCC_EMP_REFERRAL_TO", "RECR_OCC_CAMPUS_PLACEMENT_FROM",
    "RECR_OCC_CAMPUS_PLACEMENT_TO", "RECR_OCC_LOCAL_NEWSPAPER_FROM", "RECR_OCC_LOCAL_NEWSPAPER_TO",
    "RECR_OCC_RADIO_AD_FROM", "RECR_OCC_RADIO_AD_TO",
    "NOTICE_POST_BARGAIN_REP", "NOTICE_POST_BARGAIN_REP_PHYSICAL", "NOTICE_POST_BARGAIN_REP_ELECTRONIC",
    "NOTICE_POST_BARGAIN_REP_INHOUSE", "NOTICE_POST_BARGAIN_REP_PRIVATE", "NOTICE_POST_EMP_NOT_POSTED",
    "EMP_CERTIFY_COMPLIANCE", "DECL_PREP_LAST_NAME", "DECL_PREP_FIRST_NAME", "DECL_PREP_MIDDLE_NAME",
    "DECL_PREP_LAWFIRM_FEIN", "DECL_PREP_FIRM_BUSINESS_NAME", "DECL_PREP_EMAIL",
    "YEAR", "FORM_TYPE",
]

# ---------------------------------------------------------
# MAPPINGS: 
# note from aidan, still need to edit 2024_old and 2025_old and maybe 2023 because I did not do my due diligence yet.
# ---------------------------------------------------------
MAPPINGS = {}
MAPPINGS["2025_new"] = {

    "case_number": "CASE_NUMBER",
    "case_status": "CASE_STATUS",
    "received_date": "RECEIVED_DATE",
    "decision_date": "DECISION_DATE",

    "occupation_type": "OCCUPATION_TYPE",

    "emp_business_name": "EMP_BUSINESS_NAME",
    "emp_trade_name": "EMP_TRADE_NAME",

    "emp_addr1": "EMP_ADDR1",
    "emp_addr2": "EMP_ADDR2",
    "emp_city": "EMP_CITY",
    "emp_state": "EMP_STATE",
    "emp_postcode": "EMP_POSTCODE",
    "emp_country": "EMP_COUNTRY",
    "emp_province": "EMP_PROVINCE",
    "emp_phone": "EMP_PHONE",
    "emp_phoneext": "EMP_PHONEEXT",
    "emp_fein": "EMP_FEIN",

    "emp_naics": "EMP_NAICS",
    "emp_num_payroll": "EMP_NUM_PAYROLL",
    "emp_year_commenced": "EMP_YEAR_COMMENCED",

    "emp_worker_interest": "EMP_WORKER_INTEREST",
    "emp_relationship_worker": "EMP_RELATIONSHIP_WORKER",

    "emp_poc_last_name": "EMP_POC_LAST_NAME",
    "emp_poc_first_name": "EMP_POC_FIRST_NAME",
    "emp_poc_middle_name": "EMP_POC_MIDDLE_NAME",
    "emp_poc_job_title": "EMP_POC_JOB_TITLE",

    "emp_poc_addr1": "EMP_POC_ADDR1",
    "emp_poc_addr2": "EMP_POC_ADDR2",
    "emp_poc_city": "EMP_POC_CITY",
    "emp_poc_state": "EMP_POC_STATE",
    "emp_poc_postal_code": "EMP_POC_POSTAL_CODE",
    "emp_poc_country": "EMP_POC_COUNTRY",
    "emp_poc_province": "EMP_POC_PROVINCE",
    "emp_poc_phone": "EMP_POC_PHONE",
    "emp_poc_phoneext": "EMP_POC_PHONEEXT",
    "emp_poc_email": "EMP_POC_EMAIL",

    "atty_ag_rep_type": "ATTY_AG_REP_TYPE",
    "atty_ag_last_name": "ATTY_AG_LAST_NAME",
    "atty_ag_first_name": "ATTY_AG_FIRST_NAME",
    "atty_ag_middle_name": "ATTY_AG_MIDDLE_NAME",
    "atty_ag_address1": "ATTY_AG_ADDRESS1",
    "atty_ag_address2": "ATTY_AG_ADDRESS2",
    "atty_ag_city": "ATTY_AG_CITY",
    "atty_ag_state": "ATTY_AG_STATE",
    "atty_ag_postal_code": "ATTY_AG_POSTAL_CODE",
    "atty_ag_country": "ATTY_AG_COUNTRY",
    "atty_ag_province": "ATTY_AG_PROVINCE",
    "atty_ag_phone": "ATTY_AG_PHONE",
    "atty_ag_phone_ext": "ATTY_AG_PHONE_EXT",
    "atty_ag_email": "ATTY_AG_EMAIL",
    "atty_ag_law_firm_name": "ATTY_AG_LAW_FIRM_NAME",
    "atty_ag_fein": "ATTY_AG_FEIN",
    "atty_ag_state_bar_number": "ATTY_AG_STATE_BAR_NUMBER",
    "atty_ag_good_standing_state": "ATTY_AG_GOOD_STANDING_STATE",
    "atty_ag_good_standing_court": "ATTY_AG_GOOD_STANDING_COURT",

    "fw_info_appx_a_attached": "FW_INFO_APPX_A_ATTACHED",
    "fw_info_atty_or_agent": "FW_INFO_ATTY_OR_AGENT",

    "job_opp_pwd_number": "JOB_OPP_PWD_NUMBER",
    "job_title": "JOB_TITLE",
    "job_opp_pwd_attached": "JOB_OPP_PWD_ATTACHED",
    "job_opp_wage_from": "JOB_OPP_WAGE_FROM",
    "job_opp_wage_to": "JOB_OPP_WAGE_TO",
    "job_opp_wage_per": "JOB_OPP_WAGE_PER",
    "job_opp_wage_conditions": "JOB_OPP_WAGE_CONDITIONS",

    "primary_worksite_type": "PRIMARY_WORKSITE_TYPE",
    "primary_worksite_addr1": "PRIMARY_WORKSITE_ADDR1",
    "primary_worksite_addr2": "PRIMARY_WORKSITE_ADDR2",
    "primary_worksite_city": "PRIMARY_WORKSITE_CITY",
    "primary_worksite_county": "PRIMARY_WORKSITE_COUNTY",
    "primary_worksite_state": "PRIMARY_WORKSITE_STATE",
    "primary_worksite_postal_code": "PRIMARY_WORKSITE_POSTAL_CODE",
    "primary_worksite_bls_area": "PRIMARY_WORKSITE_BLS_AREA",

    "is_multiple_locations": "IS_MULTIPLE_LOCATIONS",
    "is_appendix_b_attached": "IS_APPENDIX_B_ATTACHED",

    "other_req_is_fulltime_emp": "OTHER_REQ_IS_FULLTIME_EMP",
    "other_req_is_livein_household": "OTHER_REQ_IS_LIVEIN_HOUSEHOLD",
    "other_req_is_paid_experience": "OTHER_REQ_IS_PAID_EXPERIENCE",
    "other_req_is_fw_executed_cont": "OTHER_REQ_IS_FW_EXECUTED_CONT",
    "other_req_is_emp_provided_cont": "OTHER_REQ_IS_EMP_PROVIDED_CONT",
    "other_req_accept_diploma_pwd": "OTHER_REQ_ACCEPT_DIPLOMA_PWD",
    "other_req_is_fw_currently_wrk": "OTHER_REQ_IS_FW_CURRENTLY_WRK",
    "other_req_is_fw_qualify": "OTHER_REQ_IS_FW_QUALIFY",
    "other_req_emp_will_accept": "OTHER_REQ_EMP_WILL_ACCEPT",
    "other_req_emp_rely_exp": "OTHER_REQ_EMP_RELY_EXP",
    "other_req_fw_gain_exp": "OTHER_REQ_FW_GAIN_EXP",
    "other_req_emp_pay_education": "OTHER_REQ_EMP_PAY_EDUCATION",
    "other_req_job_emp_premises": "OTHER_REQ_JOB_EMP_PREMISES",
    "other_req_job_combo_occup": "OTHER_REQ_JOB_COMBO_OCCUP",
    "other_req_job_foreign_language": "OTHER_REQ_JOB_FOREIGN_LANGUAGE",
    "other_req_job_req_exceed": "OTHER_REQ_JOB_REQ_EXCEED",
    "other_req_emp_use_credential": "OTHER_REQ_EMP_USE_CREDENTIAL",
    "other_req_emp_rec_payment": "OTHER_REQ_EMP_REC_PAYMENT",
    "other_req_emp_layoff": "OTHER_REQ_EMP_LAYOFF",

    "recr_info_recruit_supervised_req": "RECR_INFO_RECRUIT_SUPERVISED_REQ",
    "recr_info_job_start_date": "RECR_INFO_JOB_START_DATE",
    "recr_info_job_end_date": "RECR_INFO_JOB_END_DATE",
    "recr_info_is_newspaper_sunday": "RECR_INFO_IS_NEWSPAPER_SUNDAY",
    "recr_info_newspaper_name": "RECR_INFO_NEWSPAPER_NAME",
    "recr_info_ad_date1": "RECR_INFO_AD_DATE1",
    "recr_info_recruit_ad_type": "RECR_INFO_RECRUIT_AD_TYPE",
    "recr_info_newspaper_name2": "RECR_INFO_NEWSPAPER_NAME2",
    "recr_info_ad_date2": "RECR_INFO_AD_DATE2",

    "recr_occ_job_fair_from": "RECR_OCC_JOB_FAIR_FROM",
    "recr_occ_job_fair_to": "RECR_OCC_JOB_FAIR_TO",
    "recr_occ_emp_website_from": "RECR_OCC_EMP_WEBSITE_FROM",
    "recr_occ_emp_website_to": "RECR_OCC_EMP_WEBSITE_TO",
    "recr_occ_job_search_from": "RECR_OCC_JOB_SEARCH_FROM",
    "recr_occ_job_search_to": "RECR_OCC_JOB_SEARCH_TO",
    "recr_occ_on_campus_from": "RECR_OCC_ON_CAMPUS_FROM",
    "recr_occ_on_campus_to": "RECR_OCC_ON_CAMPUS_TO",
    "recr_occ_trade_org_from": "RECR_OCC_TRADE_ORG_FROM",
    "recr_occ_trade_org_to": "RECR_OCC_TRADE_ORG_TO",
    "recr_occ_private_emp_from": "RECR_OCC_PRIVATE_EMP_FROM",
    "recr_occ_private_emp_to": "RECR_OCC_PRIVATE_EMP_TO",
    "recr_occ_emp_referral_from": "RECR_OCC_EMP_REFERRAL_FROM",
    "recr_occ_emp_referral_to": "RECR_OCC_EMP_REFERRAL_TO",
    "recr_occ_campus_placement_from": "RECR_OCC_CAMPUS_PLACEMENT_FROM",
    "recr_occ_campus_placement_to": "RECR_OCC_CAMPUS_PLACEMENT_TO",
    "recr_occ_local_newspaper_from": "RECR_OCC_LOCAL_NEWSPAPER_FROM",
    "recr_occ_local_newspaper_to": "RECR_OCC_LOCAL_NEWSPAPER_TO",
    "recr_occ_radio_ad_from": "RECR_OCC_RADIO_AD_FROM",
    "recr_occ_radio_ad_to": "RECR_OCC_RADIO_AD_TO",

    "notice_post_bargain_rep": "NOTICE_POST_BARGAIN_REP",
    "notice_post_bargain_rep_physical": "NOTICE_POST_BARGAIN_REP_PHYSICAL",
    "notice_post_bargain_rep_electronic": "NOTICE_POST_BARGAIN_REP_ELECTRONIC",
    "notice_post_bargain_rep_inhouse": "NOTICE_POST_BARGAIN_REP_INHOUSE",
    "notice_post_bargain_rep_private": "NOTICE_POST_BARGAIN_REP_PRIVATE",
    "notice_post_emp_not_posted": "NOTICE_POST_EMP_NOT_POSTED",

    "emp_certify_compliance": "EMP_CERTIFY_COMPLIANCE",

    "decl_prep_last_name": "DECL_PREP_LAST_NAME",
    "decl_prep_first_name": "DECL_PREP_FIRST_NAME",
    "decl_prep_middle_name": "DECL_PREP_MIDDLE_NAME",
    "decl_prep_lawfirm_fein": "DECL_PREP_LAWFIRM_FEIN",
    "decl_prep_firm_business_name": "DECL_PREP_FIRM_BUSINESS_NAME",
    "decl_prep_email": "DECL_PREP_EMAIL",
}
MAPPINGS["2025_old"] = {

    "case_number": "CASE_NUMBER",
    "case_status": "CASE_STATUS",
    "received_date": "RECEIVED_DATE",
    "decision_date": "DECISION_DATE",

    "refile": None,  # No equivalent in final schema
    "orig_file_date": None,  # Original filing date not supported in final schema
    "previous_swa_case_number_state": None,  # No column in final schema
    "schd_a_sheepherder": None,  # Final schema does not track Schedule A flag

    "employer_name": "EMP_BUSINESS_NAME",
    "employer_address_1": "EMP_ADDR1",
    "employer_address_2": "EMP_ADDR2",
    "employer_city": "EMP_CITY",
    "employer_state_province": "EMP_STATE",
    "employer_country": "EMP_COUNTRY",
    "employer_postal_code": "EMP_POSTCODE",
    "employer_phone": "EMP_PHONE",
    "employer_phone_ext": "EMP_PHONEEXT",

    "employer_num_employees": "EMP_NUM_PAYROLL",
    "employer_year_commenced_business": "EMP_YEAR_COMMENCED",

    "employer_fein": "EMP_FEIN",
    "naics_code": "EMP_NAICS",

    "fw_ownership_interest": "EMP_WORKER_INTEREST",  # Ownership/relationship best fit
    "emp_relationship_worker": None,  # Not present in old form

    # Cannot split name → map to LAST_NAME placeholder
    "emp_contact_name": "EMP_POC_LAST_NAME",  # Best effort: combined name cannot be parsed

    "emp_contact_address_1": "EMP_POC_ADDR1",
    "emp_contact_address_2": "EMP_POC_ADDR2",
    "emp_contact_city": "EMP_POC_CITY",
    "emp_contact_state_province": "EMP_POC_STATE",
    "emp_contact_country": "EMP_POC_COUNTRY",
    "emp_contact_postal_code": "EMP_POC_POSTAL_CODE",
    "emp_contact_phone": "EMP_POC_PHONE",
    "emp_contact_email": "EMP_POC_EMAIL",

    # Attorney fields: name cannot be split, map to last_name placeholder
    "agent_attorney_name": "ATTY_AG_LAST_NAME",  # Best possible semantic match
    "agent_attorney_firm_name": "ATTY_AG_LAW_FIRM_NAME",
    "agent_attorney_phone": "ATTY_AG_PHONE",
    "agent_attorney_phone_ext": "ATTY_AG_PHONE_EXT",
    "agent_attorney_address_1": "ATTY_AG_ADDRESS1",
    "agent_attorney_address_2": "ATTY_AG_ADDRESS2",
    "agent_attorney_city": "ATTY_AG_CITY",
    "agent_attorney_state_province": "ATTY_AG_STATE",
    "agent_attorney_country": "ATTY_AG_COUNTRY",
    "agent_attorney_postal_code": "ATTY_AG_POSTAL_CODE",
    "agent_attorney_email": "ATTY_AG_EMAIL",

    "atty_ag_rep_type": None,
    "atty_ag_fein": None,
    "atty_ag_state_bar_number": None,
    "atty_ag_good_standing_state": None,
    "atty_ag_good_standing_court": None,

    "pw_track_number": "JOB_OPP_PWD_NUMBER",  # Direct match
    "pw_soc_code": None,
    "pw_soc_title": None,
    "pw_skill_level": None,
    "pw_wage": None,
    "pw_unit_of_pay": None,
    "pw_wage_source": None,
    "pw_source_name_other": None,
    "pw_determination_date": None,
    "pw_expiration_date": None,

    "wage_offer_from": "JOB_OPP_WAGE_FROM",
    "wage_offer_to": "JOB_OPP_WAGE_TO",
    "wage_offer_unit_of_pay": "JOB_OPP_WAGE_PER",

    "worksite_address_1": "PRIMARY_WORKSITE_ADDR1",
    "worksite_address_2": "PRIMARY_WORKSITE_ADDR2",
    "worksite_city": "PRIMARY_WORKSITE_CITY",
    "worksite_state": "PRIMARY_WORKSITE_STATE",
    "worksite_postal_code": "PRIMARY_WORKSITE_POSTAL_CODE",

    "primary_worksite_county": None,  # Not present in old form
    "primary_worksite_bls_area": None,
    "is_multiple_locations": None,
    "is_appendix_b_attached": None,

    "job_title": "JOB_TITLE",

    "minimum_education": None,
    "job_education_min_other": None,
    "major_field_of_study": None,
    "required_training": None,
    "required_training_months": None,
    "required_field_of_training": None,
    "required_experience": None,
    "required_experience_months": None,
    "accept_alt_field_of_study": None,
    "accept_alt_major_fld_of_study": None,
    "accept_alt_combo": None,
    "accept_alt_combo_education": None,
    "accept_alt_combo_ed_other": None,
    "accept_alt_combo_education_yrs": None,
    "accept_foreign_education": None,
    "accept_alt_occupation": None,
    "accept_alt_occupation_months": None,
    "accept_alt_job_title": None,
    "job_opp_requirements_normal": None,
    "foreign_language_required": None,
    "specific_skills": None,
    "combination_occupation": None,
    "offered_to_appl_foreign_worker": None,

    "professional_occupation": None,
    "app_for_college_u_teacher": None,
    "competitive_process": None,
    "basic_recruitment_process": None,
    "teacher_select_date": None,
    "teacher_pub_journal_name": None,
    "add_recruit_information": None,

    "foreign_worker_live_on_prem": None,
    "foreign_worker_live_in_dom_ser": None,
    "foreign_worker_live_in_dom_svc_cnt": None,

    "country_of_citizenship": None,
    "foreign_worker_birth_country": None,
    "class_of_admission": None,
    "foreign_worker_education": None,
    "foreign_worker_education_oth er": None,
    "foreign_worker_info_major": None,
    "foreign_worker_yrs_ed_comp": None,
    "foreign_worker_inst_of_ed": None,
    "foreign_worker_ed_inst_add_1": None,
    "foreign_worker_ed_inst_add_2": None,
    "foreign_worker_ed_inst_city": None,
    "foreign_worker_ed_inst_state_p": None,
    "foreign_worker_ed_inst_country": None,
    "foreign_worker_ed_inst_post_cd": None,
    "foreign_worker_training_comp": None,
    "foreign_worker_req_experience": None,
    "foreign_worker_alt_ed_exp": None,
    "foreign_worker_alt_occ_exp": None,
    "foreign_worker_exp_with_empl": None,
    "foreign_worker_empl_pay_for_ed": None,
    "foreign_worker_curr_employed": None,

    "swa_job_order_start_date": None,
    "swa_job_order_end_date": None,
    "sunday_edition_newspaper": None,
    "first_newspaper_name": None,
    "first_advertisement_start_date": None,
    "second_newspaper_ad_name": None,
    "second_advertisement_type": None,
    "second_ad_start_date": None,
    "job_fair_from_date": None,
    "job_fair_to_date": None,
    "on_campus_recruiting_from_date": None,
    "on_campus_recruiting_to_date": None,
    "employer_website_from_date": None,
    "employer_website_to_date": None,
    "pro_org_ad_from_date": None,
    "pro_org_advertisement_to_date": None,
    "job_search_website_from_date": None,
    "job_search_website_to_date": None,
    "pvt_employment_firm_from_date": None,
    "pvt_employment_firm_to_date": None,
    "employee_ref_prog_from_date": None,
    "employee_referral_prog_to_date": None,
    "campus_placement_from_date": None,
    "campus_placement_to_date": None,
    "local_ethnic_paper_from_date": None,
    "local_ethnic_paper_to_date": None,
    "radio_tv_ad_from_date": None,
    "radio_tv_ad_to_date": None,

    "emp_received_payment": "OTHER_REQ_EMP_REC_PAYMENT",  # Best semantic linkage
    "payment_details": None,
    "bargaining_rep_notified": "NOTICE_POST_BARGAIN_REP",  # Closest conceptual match
    "posted_notice_at_worksite": "NOTICE_POST_BARGAIN_REP_PHYSICAL",  # Best effort
    "layoff_in_past_six_months": "OTHER_REQ_EMP_LAYOFF",  # Layoff flag matches perfectly
    "us_workers_considered": None,

    # Employer declaration
    "employer_completed_application": "EMP_CERTIFY_COMPLIANCE",  # Best possible match

    # Preparer info — name cannot be split → map to last_name placeholder
    "preparer_name": "DECL_PREP_LAST_NAME",
    "preparer_title": None,
    "preparer_email": "DECL_PREP_EMAIL",

    # Declaration signer → not available in final schema
    "emp_info_decl_name": None,
    "emp_decl_title": None,
}
MAPPINGS["2024_old"] = {
    "case_number": "CASE_NUMBER",
    "case_status": "CASE_STATUS",
    "received_date": "RECEIVED_DATE",
    "decision_date": "DECISION_DATE",
    "refile": None,
    "orig_file_date": None,
    "previous_swa_case_number_state": None,
    "schd_a_sheepherder": None,
    "employer_name": "EMP_BUSINESS_NAME",
    "employer_address_1": "EMP_ADDR1",
    "employer_address_2": "EMP_ADDR2",
    "employer_city": "EMP_CITY",
    "employer_state_province": "EMP_STATE",
    "employer_country": "EMP_COUNTRY",
    "employer_postal_code": "EMP_POSTCODE",
    "employer_phone": "EMP_PHONE",
    "employer_phone_ext": "EMP_PHONEEXT",
    "employer_fein": "EMP_FEIN",
    "naics_code": "EMP_NAICS",
    "employer_num_employees": "EMP_NUM_PAYROLL",
    "employer_year_commenced_business": "EMP_YEAR_COMMENCED",
    # Ownership matches FINAL_SCHEMA exactly:
    "fw_ownership_interest": "EMP_WORKER_INTEREST",
    # Old form does NOT include employee-family relationship
    "emp_relationship_worker": None,
    # Old form provides NAME as one field;
    # we cannot split it perfectly → map to LAST_NAME to avoid weirdness.
    "emp_contact_name": "EMP_POC_LAST_NAME",  # BEST EFFORT (cannot extract first/middle)
    "emp_contact_address_1": "EMP_POC_ADDR1",
    "emp_contact_address_2": "EMP_POC_ADDR2",
    "emp_contact_city": "EMP_POC_CITY",
    "emp_contact_state_province": "EMP_POC_STATE",
    "emp_contact_country": "EMP_POC_COUNTRY",
    "emp_contact_postal_code": "EMP_POC_POSTAL_CODE",
    "emp_contact_phone": "EMP_POC_PHONE",
    "emp_contact_email": "EMP_POC_EMAIL",
    # Name is combined → map to LAW_FIRM_NAME (best approximation)
    "agent_attorney_name": "ATTY_AG_LAST_NAME",  
    "agent_attorney_firm_name": "ATTY_AG_LAW_FIRM_NAME",
    "agent_attorney_phone": "ATTY_AG_PHONE",
    "agent_attorney_phone_ext": "ATTY_AG_PHONE_EXT",
    "agent_attorney_address_1": "ATTY_AG_ADDRESS1",
    "agent_attorney_address_2": "ATTY_AG_ADDRESS2",
    "agent_attorney_city": "ATTY_AG_CITY",
    "agent_attorney_state_province": "ATTY_AG_STATE",
    "agent_attorney_country": "ATTY_AG_COUNTRY",
    "agent_attorney_postal_code": "ATTY_AG_POSTAL_CODE",
    "agent_attorney_email": "ATTY_AG_EMAIL",
    # Missing from old form:
    "atty_ag_rep_type": None,
    "atty_ag_fein": None,
    "atty_ag_state_bar_number": None,
    "atty_ag_good_standing_state": None,
    "atty_ag_good_standing_court": None,
    "pw_track_number": "JOB_OPP_PWD_NUMBER",  # direct match
    # FINAL_SCHEMA does NOT accept all these → drop:
    "pw_soc_code": None,
    "pw_soc_title": None,
    "pw_skill_level": None,
    "pw_wage": None,
    "pw_unit_of_pay": None,
    "pw_wage_source": None,
    "pw_source_name_other": None,
    "pw_determination_date": None,
    "pw_expiration_date": None,
    "wage_offer_from": "JOB_OPP_WAGE_FROM",
    "wage_offer_to": "JOB_OPP_WAGE_TO",
    "wage_offer_unit_of_pay": "JOB_OPP_WAGE_PER",
    "worksite_address_1": "PRIMARY_WORKSITE_ADDR1",
    "worksite_address_2": "PRIMARY_WORKSITE_ADDR2",
    "worksite_city": "PRIMARY_WORKSITE_CITY",
    "worksite_state": "PRIMARY_WORKSITE_STATE",
    "worksite_postal_code": "PRIMARY_WORKSITE_POSTAL_CODE",
    "primary_worksite_county": None,
    "primary_worksite_bls_area": None,
    "is_multiple_locations": None,
    "is_appendix_b_attached": None,
    "job_title": "JOB_TITLE",
    "minimum_education": None,
    "job_education_min_other": None,
    "major_field_of_study": None,
    "required_training": None,
    "required_training_months": None,
    "required_field_of_training": None,
    "required_experience": None,
    "required_experience_months": None,
    "accept_alt_field_of_study": None,
    "accept_alt_major_fld_of_study": None,
    "accept_alt_combo": None,
    "accept_alt_combo_education": None,
    "accept_alt_combo_ed_other": None,
    "accept_alt_combo_education_yrs": None,
    "accept_foreign_education": None,
    "accept_alt_occupation": None,
    "accept_alt_occupation_months": None,
    "accept_alt_job_title": None,
    "job_opp_requirements_normal": None,
    "foreign_language_required": None,
    "specific_skills": None,
    "combination_occupation": None,
    "offered_to_appl_foreign_worker": None,
    "professional_occupation": None,
    "app_for_college_u_teacher": None,
    "competitive_process": None,
    "basic_recruitment_process": None,
    "teacher_select_date": None,
    "teacher_pub_journal_name": None,
    "add_recruit_information": None,
    "foreign_worker_live_on_prem": None,
    "foreign_worker_live_in_dom_ser": None,
    "foreign_worker_live_in_dom_svc_cnt": None,
    "country_of_citizenship": None,
    "foreign_worker_birth_country": None,
    "class_of_admission": None,
    "foreign_worker_education": None,
    "foreign_worker_education_other": None,
    "foreign_worker_info_major": None,
    "foreign_worker_yrs_ed_comp": None,
    "foreign_worker_inst_of_ed": None,
    "foreign_worker_ed_inst_add_1": None,
    "foreign_worker_ed_inst_add_2": None,
    "foreign_worker_ed_inst_city": None,
    "foreign_worker_ed_inst_state_p": None,
    "foreign_worker_ed_inst_country": None,
    "foreign_worker_ed_inst_post_cd": None,
    "foreign_worker_training_comp": None,
    "foreign_worker_req_experience": None,
    "foreign_worker_alt_ed_exp": None,
    "foreign_worker_alt_occ_exp": None,
    "foreign_worker_exp_with_empl": None,
    "foreign_worker_empl_pay_for_ed": None,
    "foreign_worker_curr_employed": None,
    "swa_job_order_start_date": None,
    "swa_job_order_end_date": None,
    "sunday_edition_newspaper": None,
    "first_newspaper_name": None,
    "first_advertisement_start_date": None,
    "second_newspaper_ad_name": None,
    "second_advertisement_type": None,
    "second_ad_start_date": None,
    "job_fair_from_date": None,
    "job_fair_to_date": None,
    "on_campus_recruiting_from_date": None,
    "on_campus_recruiting_to_date": None,
    "employer_website_from_date": None,
    "employer_website_to_date": None,
    "pro_org_ad_from_date": None,
    "pro_org_advertisement_to_date": None,
    "job_search_website_from_date": None,
    "job_search_website_to_date": None,
    "pvt_employment_firm_from_date": None,
    "pvt_employment_firm_to_date": None,
    "employee_ref_prog_from_date": None,
    "employee_referral_prog_to_date": None,
    "campus_placement_from_date": None,
    "campus_placement_to_date": None,
    "local_ethnic_paper_from_date": None,
    "local_ethnic_paper_to_date": None,
    "radio_tv_ad_from_date": None,
    "radio_tv_ad_to_date": None,
    "emp_received_payment": "OTHER_REQ_EMP_REC_PAYMENT",  # BEST EFFORT (payment relates to employer)
    "payment_details": None,
    "bargaining_rep_notified": "NOTICE_POST_BARGAIN_REP", 
    "posted_notice_at_worksite": "NOTICE_POST_BARGAIN_REP_PHYSICAL",  
    "layoff_in_past_six_months": "OTHER_REQ_EMP_LAYOFF",  # direct concept match
    "employer_completed_application": "EMP_CERTIFY_COMPLIANCE",  # BEST possible match
    "preparer_name": "DECL_PREP_LAST_NAME",  # cannot split → map to last name
    "preparer_title": None,
    "preparer_email": "DECL_PREP_EMAIL",
}
MAPPINGS["2024_new"] = {
    "case_number": "CASE_NUMBER",
    "case_status": "CASE_STATUS",
    "received_date": "RECEIVED_DATE",
    "decision_date": "DECISION_DATE",
    "occupation_type": "OCCUPATION_TYPE",
    "emp_business_name": "EMP_BUSINESS_NAME",
    "emp_trade_name": "EMP_TRADE_NAME",
    "emp_addr1": "EMP_ADDR1",
    "emp_addr2": "EMP_ADDR2",
    "emp_city": "EMP_CITY",
    "emp_state": "EMP_STATE",
    "emp_postcode": "EMP_POSTCODE",
    "emp_country": "EMP_COUNTRY",
    "emp_province": "EMP_PROVINCE",
    "emp_phone": "EMP_PHONE",
    "emp_phoneext": "EMP_PHONEEXT",
    "emp_fein": "EMP_FEIN",
    "emp_naics": "EMP_NAICS",
    "emp_num_payroll": "EMP_NUM_PAYROLL",
    "emp_year_commenced": "EMP_YEAR_COMMENCED",
    "emp_worker_interest": "EMP_WORKER_INTEREST",
    "emp_relationship_worker": "EMP_RELATIONSHIP_WORKER",
    "emp_poc_last_name": "EMP_POC_LAST_NAME",
    "emp_poc_first_name": "EMP_POC_FIRST_NAME",
    "emp_poc_middle_name": "EMP_POC_MIDDLE_NAME",
    "emp_poc_job_title": "EMP_POC_JOB_TITLE",
    "emp_poc_addr1": "EMP_POC_ADDR1",
    "emp_poc_addr2": "EMP_POC_ADDR2",
    "emp_poc_city": "EMP_POC_CITY",
    "emp_poc_state": "EMP_POC_STATE",
    "emp_poc_postal_code": "EMP_POC_POSTAL_CODE",
    "emp_poc_country": "EMP_POC_COUNTRY",
    "emp_poc_province": "EMP_POC_PROVINCE",
    "emp_poc_phone": "EMP_POC_PHONE",
    "emp_poc_phoneext": "EMP_POC_PHONEEXT",
    "emp_poc_email": "EMP_POC_EMAIL",
    "atty_ag_rep_type": "ATTY_AG_REP_TYPE",
    "atty_ag_last_name": "ATTY_AG_LAST_NAME",
    "atty_ag_first_name": "ATTY_AG_FIRST_NAME",
    "atty_ag_middle_name": "ATTY_AG_MIDDLE_NAME",
    "atty_ag_address1": "ATTY_AG_ADDRESS1",
    "atty_ag_address2": "ATTY_AG_ADDRESS2",
    "atty_ag_city": "ATTY_AG_CITY",
    "atty_ag_state": "ATTY_AG_STATE",
    "atty_ag_postal_code": "ATTY_AG_POSTAL_CODE",
    "atty_ag_country": "ATTY_AG_COUNTRY",
    "atty_ag_province": "ATTY_AG_PROVINCE",
    "atty_ag_phone": "ATTY_AG_PHONE",
    "atty_ag_phone_ext": "ATTY_AG_PHONE_EXT",
    "atty_ag_email": "ATTY_AG_EMAIL",
    "atty_ag_law_firm_name": "ATTY_AG_LAW_FIRM_NAME",
    "atty_ag_fein": "ATTY_AG_FEIN",
    "atty_ag_state_bar_number": "ATTY_AG_STATE_BAR_NUMBER",
    "atty_ag_good_standing_state": "ATTY_AG_GOOD_STANDING_STATE",
    "atty_ag_good_standing_court": "ATTY_AG_GOOD_STANDING_COURT",
    "fw_info_appx_a_attached": "FW_INFO_APPX_A_ATTACHED",
    "fw_info_atty_or_agent": "FW_INFO_ATTY_OR_AGENT",
    "job_opp_pwd_number": "JOB_OPP_PWD_NUMBER",
    "job_title": "JOB_TITLE",
    "job_opp_pwd_attached": "JOB_OPP_PWD_ATTACHED",
    "job_opp_wage_from": "JOB_OPP_WAGE_FROM",
    "job_opp_wage_to": "JOB_OPP_WAGE_TO",
    "job_opp_wage_per": "JOB_OPP_WAGE_PER",
    "job_opp_wage_conditions": "JOB_OPP_WAGE_CONDITIONS",
    "primary_worksite_type": "PRIMARY_WORKSITE_TYPE",
    "primary_worksite_addr1": "PRIMARY_WORKSITE_ADDR1",
    "primary_worksite_addr2": "PRIMARY_WORKSITE_ADDR2",
    "primary_worksite_city": "PRIMARY_WORKSITE_CITY",
    "primary_worksite_county": "PRIMARY_WORKSITE_COUNTY",
    "primary_worksite_state": "PRIMARY_WORKSITE_STATE",
    "primary_worksite_postal_code": "PRIMARY_WORKSITE_POSTAL_CODE",
    "primary_worksite_bls_area": "PRIMARY_WORKSITE_BLS_AREA",
    "is_multiple_locations": "IS_MULTIPLE_LOCATIONS",
    "is_appendix_b_attached": "IS_APPENDIX_B_ATTACHED",
    "other_req_is_fulltime_emp": "OTHER_REQ_IS_FULLTIME_EMP",
    "other_req_is_livein_household": "OTHER_REQ_IS_LIVEIN_HOUSEHOLD",
    "other_req_is_paid_experience": "OTHER_REQ_IS_PAID_EXPERIENCE",
    "other_req_is_fw_executed_cont": "OTHER_REQ_IS_FW_EXECUTED_CONT",
    "other_req_is_emp_provided_cont": "OTHER_REQ_IS_EMP_PROVIDED_CONT",
    "other_req_accept_diploma_pwd": "OTHER_REQ_ACCEPT_DIPLOMA_PWD",
    "other_req_is_fw_currently_wrk": "OTHER_REQ_IS_FW_CURRENTLY_WRK",
    "other_req_is_fw_qualify": "OTHER_REQ_IS_FW_QUALIFY",
    "other_req_emp_will_accept": "OTHER_REQ_EMP_WILL_ACCEPT",
    "other_req_emp_rely_exp": "OTHER_REQ_EMP_RELY_EXP",
    "other_req_fw_gain_exp": "OTHER_REQ_FW_GAIN_EXP",
    "other_req_emp_pay_education": "OTHER_REQ_EMP_PAY_EDUCATION",
    "other_req_job_emp_premises": "OTHER_REQ_JOB_EMP_PREMISES",
    "other_req_job_combo_occup": "OTHER_REQ_JOB_COMBO_OCCUP",
    "other_req_job_foreign_language": "OTHER_REQ_JOB_FOREIGN_LANGUAGE",
    "other_req_job_req_exceed": "OTHER_REQ_JOB_REQ_EXCEED",
    "other_req_emp_use_credential": "OTHER_REQ_EMP_USE_CREDENTIAL",
    "other_req_emp_rec_payment": "OTHER_REQ_EMP_REC_PAYMENT",
    "other_req_emp_layoff": "OTHER_REQ_EMP_LAYOFF",
    "recr_info_recruit_supervised_req": "RECR_INFO_RECRUIT_SUPERVISED_REQ",
    "recr_info_job_start_date": "RECR_INFO_JOB_START_DATE",
    "recr_info_job_end_date": "RECR_INFO_JOB_END_DATE",
    "recr_info_is_newspaper_sunday": "RECR_INFO_IS_NEWSPAPER_SUNDAY",
    "recr_info_newspaper_name": "RECR_INFO_NEWSPAPER_NAME",
    "recr_info_ad_date1": "RECR_INFO_AD_DATE1",
    "recr_info_recruit_ad_type": "RECR_INFO_RECRUIT_AD_TYPE",
    "recr_info_newspaper_name2": "RECR_INFO_NEWSPAPER_NAME2",
    "recr_info_ad_date2": "RECR_INFO_AD_DATE2",
    "recr_occ_job_fair_from": "RECR_OCC_JOB_FAIR_FROM",
    "recr_occ_job_fair_to": "RECR_OCC_JOB_FAIR_TO",
    "recr_occ_emp_website_from": "RECR_OCC_EMP_WEBSITE_FROM",
    "recr_occ_emp_website_to": "RECR_OCC_EMP_WEBSITE_TO",
    "recr_occ_job_search_from": "RECR_OCC_JOB_SEARCH_FROM",
    "recr_occ_job_search_to": "RECR_OCC_JOB_SEARCH_TO",
    "recr_occ_on_campus_from": "RECR_OCC_ON_CAMPUS_FROM",
    "recr_occ_on_campus_to": "RECR_OCC_ON_CAMPUS_TO",
    "recr_occ_trade_org_from": "RECR_OCC_TRADE_ORG_FROM",
    "recr_occ_trade_org_to": "RECR_OCC_TRADE_ORG_TO",
    "recr_occ_private_emp_from": "RECR_OCC_PRIVATE_EMP_FROM",
    "recr_occ_private_emp_to": "RECR_OCC_PRIVATE_EMP_TO",
    "recr_occ_emp_referral_from": "RECR_OCC_EMP_REFERRAL_FROM",
    "recr_occ_emp_referral_to": "RECR_OCC_EMP_REFERRAL_TO",
    "recr_occ_campus_placement_from": "RECR_OCC_CAMPUS_PLACEMENT_FROM",
    "recr_occ_campus_placement_to": "RECR_OCC_CAMPUS_PLACEMENT_TO",
    "recr_occ_local_newspaper_from": "RECR_OCC_LOCAL_NEWSPAPER_FROM",
    "recr_occ_local_newspaper_to": "RECR_OCC_LOCAL_NEWSPAPER_TO",
    "recr_occ_radio_ad_from": "RECR_OCC_RADIO_AD_FROM",
    "recr_occ_radio_ad_to": "RECR_OCC_RADIO_AD_TO",
    "notice_post_bargain_rep": "NOTICE_POST_BARGAIN_REP",
    "notice_post_bargain_rep_physical": "NOTICE_POST_BARGAIN_REP_PHYSICAL",
    "notice_post_bargain_rep_electronic": "NOTICE_POST_BARGAIN_REP_ELECTRONIC",
    "notice_post_bargain_rep_inhouse": "NOTICE_POST_BARGAIN_REP_INHOUSE",
    "notice_post_bargain_rep_private": "NOTICE_POST_BARGAIN_REP_PRIVATE",
    "notice_post_emp_not_posted": "NOTICE_POST_EMP_NOT_POSTED",
    "emp_certify_compliance": "EMP_CERTIFY_COMPLIANCE",
    "decl_prep_last_name": "DECL_PREP_LAST_NAME",
    "decl_prep_first_name": "DECL_PREP_FIRST_NAME",
    "decl_prep_middle_name": "DECL_PREP_MIDDLE_NAME",
    "decl_prep_lawfirm_fein": "DECL_PREP_LAWFIRM_FEIN",
    "decl_prep_firm_business_name": "DECL_PREP_FIRM_BUSINESS_NAME",
    "decl_prep_email": "DECL_PREP_EMAIL",
}
# 2023 same as 2024_old except without EMPLOYER_FEIN
MAPPINGS["2023"] = {
    "case_number": "CASE_NUMBER",
    "case_status": "CASE_STATUS",
    "received_date": "RECEIVED_DATE",
    "decision_date": "DECISION_DATE",
    "refile": None,
    "orig_file_date": None,
    "previous_swa_case_number_state": None,
    "schd_a_sheepherder": None,
    "employer_name": "EMP_BUSINESS_NAME",
    "employer_address_1": "EMP_ADDR1",
    "employer_address_2": "EMP_ADDR2",
    "employer_city": "EMP_CITY",
    "employer_state_province": "EMP_STATE",
    "employer_country": "EMP_COUNTRY",
    "employer_postal_code": "EMP_POSTCODE",
    "employer_phone": "EMP_PHONE",
    "employer_phone_ext": "EMP_PHONEEXT",
    "naics_code": "EMP_NAICS",
    "employer_num_employees": "EMP_NUM_PAYROLL",
    "employer_year_commenced_business": "EMP_YEAR_COMMENCED",
    # Ownership matches FINAL_SCHEMA exactly:
    "fw_ownership_interest": "EMP_WORKER_INTEREST",
    # Old form does NOT include employee-family relationship
    "emp_relationship_worker": None,
    # Old form provides NAME as one field;
    # we cannot split it perfectly → map to LAST_NAME to avoid weirdness.
    "emp_contact_name": "EMP_POC_LAST_NAME",  # BEST EFFORT (cannot extract first/middle)
    "emp_contact_address_1": "EMP_POC_ADDR1",
    "emp_contact_address_2": "EMP_POC_ADDR2",
    "emp_contact_city": "EMP_POC_CITY",
    "emp_contact_state_province": "EMP_POC_STATE",
    "emp_contact_country": "EMP_POC_COUNTRY",
    "emp_contact_postal_code": "EMP_POC_POSTAL_CODE",
    "emp_contact_phone": "EMP_POC_PHONE",
    "emp_contact_email": "EMP_POC_EMAIL",
    # Name is combined → map to LAW_FIRM_NAME (best approximation)
    "agent_attorney_name": "ATTY_AG_LAST_NAME",  
    "agent_attorney_firm_name": "ATTY_AG_LAW_FIRM_NAME",
    "agent_attorney_phone": "ATTY_AG_PHONE",
    "agent_attorney_phone_ext": "ATTY_AG_PHONE_EXT",
    "agent_attorney_address_1": "ATTY_AG_ADDRESS1",
    "agent_attorney_address_2": "ATTY_AG_ADDRESS2",
    "agent_attorney_city": "ATTY_AG_CITY",
    "agent_attorney_state_province": "ATTY_AG_STATE",
    "agent_attorney_country": "ATTY_AG_COUNTRY",
    "agent_attorney_postal_code": "ATTY_AG_POSTAL_CODE",
    "agent_attorney_email": "ATTY_AG_EMAIL",
    # Missing from old form:
    "atty_ag_rep_type": None,
    "atty_ag_fein": None,
    "atty_ag_state_bar_number": None,
    "atty_ag_good_standing_state": None,
    "atty_ag_good_standing_court": None,
    "pw_track_number": "JOB_OPP_PWD_NUMBER",  # direct match
    # FINAL_SCHEMA does NOT accept all these → drop:
    "pw_soc_code": None,
    "pw_soc_title": None,
    "pw_skill_level": None,
    "pw_wage": None,
    "pw_unit_of_pay": None,
    "pw_wage_source": None,
    "pw_source_name_other": None,
    "pw_determination_date": None,
    "pw_expiration_date": None,
    "wage_offer_from": "JOB_OPP_WAGE_FROM",
    "wage_offer_to": "JOB_OPP_WAGE_TO",
    "wage_offer_unit_of_pay": "JOB_OPP_WAGE_PER",
    "worksite_address_1": "PRIMARY_WORKSITE_ADDR1",
    "worksite_address_2": "PRIMARY_WORKSITE_ADDR2",
    "worksite_city": "PRIMARY_WORKSITE_CITY",
    "worksite_state": "PRIMARY_WORKSITE_STATE",
    "worksite_postal_code": "PRIMARY_WORKSITE_POSTAL_CODE",
    "primary_worksite_county": None,
    "primary_worksite_bls_area": None,
    "is_multiple_locations": None,
    "is_appendix_b_attached": None,
    "job_title": "JOB_TITLE",
    "minimum_education": None,
    "job_education_min_other": None,
    "major_field_of_study": None,
    "required_training": None,
    "required_training_months": None,
    "required_field_of_training": None,
    "required_experience": None,
    "required_experience_months": None,
    "accept_alt_field_of_study": None,
    "accept_alt_major_fld_of_study": None,
    "accept_alt_combo": None,
    "accept_alt_combo_education": None,
    "accept_alt_combo_ed_other": None,
    "accept_alt_combo_education_yrs": None,
    "accept_foreign_education": None,
    "accept_alt_occupation": None,
    "accept_alt_occupation_months": None,
    "accept_alt_job_title": None,
    "job_opp_requirements_normal": None,
    "foreign_language_required": None,
    "specific_skills": None,
    "combination_occupation": None,
    "offered_to_appl_foreign_worker": None,
    "professional_occupation": None,
    "app_for_college_u_teacher": None,
    "competitive_process": None,
    "basic_recruitment_process": None,
    "teacher_select_date": None,
    "teacher_pub_journal_name": None,
    "add_recruit_information": None,
    "foreign_worker_live_on_prem": None,
    "foreign_worker_live_in_dom_ser": None,
    "foreign_worker_live_in_dom_svc_cnt": None,
    "country_of_citizenship": None,
    "foreign_worker_birth_country": None,
    "class_of_admission": None,
    "foreign_worker_education": None,
    "foreign_worker_education_other": None,
    "foreign_worker_info_major": None,
    "foreign_worker_yrs_ed_comp": None,
    "foreign_worker_inst_of_ed": None,
    "foreign_worker_ed_inst_add_1": None,
    "foreign_worker_ed_inst_add_2": None,
    "foreign_worker_ed_inst_city": None,
    "foreign_worker_ed_inst_state_p": None,
    "foreign_worker_ed_inst_country": None,
    "foreign_worker_ed_inst_post_cd": None,
    "foreign_worker_training_comp": None,
    "foreign_worker_req_experience": None,
    "foreign_worker_alt_ed_exp": None,
    "foreign_worker_alt_occ_exp": None,
    "foreign_worker_exp_with_empl": None,
    "foreign_worker_empl_pay_for_ed": None,
    "foreign_worker_curr_employed": None,
    "swa_job_order_start_date": None,
    "swa_job_order_end_date": None,
    "sunday_edition_newspaper": None,
    "first_newspaper_name": None,
    "first_advertisement_start_date": None,
    "second_newspaper_ad_name": None,
    "second_advertisement_type": None,
    "second_ad_start_date": None,
    "job_fair_from_date": None,
    "job_fair_to_date": None,
    "on_campus_recruiting_from_date": None,
    "on_campus_recruiting_to_date": None,
    "employer_website_from_date": None,
    "employer_website_to_date": None,
    "pro_org_ad_from_date": None,
    "pro_org_advertisement_to_date": None,
    "job_search_website_from_date": None,
    "job_search_website_to_date": None,
    "pvt_employment_firm_from_date": None,
    "pvt_employment_firm_to_date": None,
    "employee_ref_prog_from_date": None,
    "employee_referral_prog_to_date": None,
    "campus_placement_from_date": None,
    "campus_placement_to_date": None,
    "local_ethnic_paper_from_date": None,
    "local_ethnic_paper_to_date": None,
    "radio_tv_ad_from_date": None,
    "radio_tv_ad_to_date": None,
    "emp_received_payment": "OTHER_REQ_EMP_REC_PAYMENT",  # BEST EFFORT (payment relates to employer)
    "payment_details": None,
    "bargaining_rep_notified": "NOTICE_POST_BARGAIN_REP", 
    "posted_notice_at_worksite": "NOTICE_POST_BARGAIN_REP_PHYSICAL",  
    "layoff_in_past_six_months": "OTHER_REQ_EMP_LAYOFF",  # direct concept match
    "employer_completed_application": "EMP_CERTIFY_COMPLIANCE",  # BEST possible match
    "preparer_name": "DECL_PREP_LAST_NAME",  # cannot split → map to last name
    "preparer_title": None,
    "preparer_email": "DECL_PREP_EMAIL",
}
MAPPINGS["2022"] = MAPPINGS["2023"]
MAPPINGS["2021"] = MAPPINGS["2023"]
MAPPINGS["2020"] = MAPPINGS["2023"]
MAPPINGS["2019"] = {
    "CASE_NUMBER": "CASE_NUMBER",  # direct match
    "DECISION_DATE": "DECISION_DATE",  # direct match
    "CASE_STATUS": "CASE_STATUS",  # direct match
    "CASE_RECEIVED_DATE": "RECEIVED_DATE",  # best semantic match
    "REFILE": None,
    "ORIG_FILE_DATE": None,
    "ORIG_CASE_NO": None,
    "SCHD_A_SHEEPHERDER": None,

    "EMPLOYER_NAME": "EMP_BUSINESS_NAME",
    "EMPLOYER_ADDRESS_1": "EMP_ADDR1",
    "EMPLOYER_ADDRESS_2": "EMP_ADDR2",
    "EMPLOYER_CITY": "EMP_CITY",
    "EMPLOYER_STATE": "EMP_STATE",
    "EMPLOYER_COUNTRY": "EMP_COUNTRY",
    "EMPLOYER_POSTAL_CODE": "EMP_POSTCODE",
    "EMPLOYER_PHONE": "EMP_PHONE",
    "EMPLOYER_PHONE_EXT": "EMP_PHONEEXT",
    "EMPLOYER_NUM_EMPLOYEES": "EMP_NUM_PAYROLL",
    "EMPLOYER_YR_ESTAB": "EMP_YEAR_COMMENCED",

    "FW_OWNERSHIP_INTEREST": "EMP_WORKER_INTEREST",  # ownership -> EMP_WORKER_INTEREST only

    "AGENT_FIRM_NAME": "ATTY_AG_LAW_FIRM_NAME",
    "AGENT_CITY": "ATTY_AG_CITY",
    "AGENT_STATE": "ATTY_AG_STATE",

    "PW_TRACK_NUM": None,
    "PW_SOC_CODE": None,
    "PW_SOC_TITLE": None,
    "PW_LEVEL": None,
    "PW_AMOUNT": None,
    "PW_UNIT_OF_PAY": None,
    "PW_SOURCE_NAME": None,
    "PW_SOURCE_NAME_OTHER": None,
    "PW_DETERM_DATE": None,
    "PW_EXPIRE_DATE": None,

    "WAGE_OFFER_FROM": "JOB_OPP_WAGE_FROM",
    "WAGE_OFFER_TO": "JOB_OPP_WAGE_TO",
    "WAGE_OFFER_UNIT_OF_PAY": "JOB_OPP_WAGE_PER",

    "JOB_INFO_WORK_CITY": "PRIMARY_WORKSITE_CITY",
    "JOB_INFO_WORK_STATE": "PRIMARY_WORKSITE_STATE",
    "JOB_INFO_WORK_POSTAL_CODE": "PRIMARY_WORKSITE_POSTAL_CODE",
    "JOB_INFO_JOB_TITLE": "JOB_TITLE",

    "JOB_INFO_EDUCATION": None,
    "JOB_INFO_EDUCATION_OTHER": None,
    "JOB_INFO_MAJOR": None,
    "JOB_INFO_TRAINING": None,
    "JOB_INFO_TRAINING_NUM_MONTHS": None,
    "JOB_INFO_TRAINING_FIELD": None,
    "JOB_INFO_EXPERIENCE": None,
    "JOB_INFO_EXPERIENCE_NUM_MONTHS": None,
    "JOB_INFO_ALT_FIELD": None,
    "JOB_INFO_ALT_FIELD_NAME": None,
    "JOB_INFO_ALT_COMBO_ED_EXP": None,
    "JOB_INFO_ALT_COMBO_ED": None,
    "JOB_INFO_ALT_COMBO_ED_OTHER": None,
    "JOB_INFO_ALT_CMB_ED_OTH_YRS": None,
    "JOB_INFO_FOREIGN_ED": None,
    "JOB_INFO_ALT_OCC": None,
    "JOB_INFO_ALT_OCC_NUM_MONTHS": None,
    "JOB_INFO_ALT_OCC_JOB_TITLE": None,
    "JOB_INFO_JOB_REQ_NORMAL": None,
    "JOB_INFO_FOREIGN_LANG_REQ": None,
    "JOB_INFO_COMBO_OCCUPATION": None,

    "JI_OFFERED_TO_SEC_J_FW": None,
    "JI_FW_LIVE_ON_PREMISES": "OTHER_REQ_JOB_EMP_PREMISES",
    "JI_LIVE_IN_DOMESTIC_SERVICE": "OTHER_REQ_IS_LIVEIN_HOUSEHOLD",
    "JI_LIVE_IN_DOM_SVC_CONTRACT": "OTHER_REQ_IS_EMP_PROVIDED_CONT",

    "RECR_INFO_PROFESSIONAL_OCC": None,
    "RECR_INFO_COLL_UNIV_TEACHER": None,
    "RECR_INFO_COLL_TEACH_COMP_PROC": None,
    "RI_COLL_TCH_BASIC_PROCESS": None,
    "RI_COLL_TEACH_SELECT_DATE": None,
    "RI_COLL_TEACH_PRO_JNL": None,

    "RECR_INFO_SWA_JOB_ORDER_START": "RECR_INFO_JOB_START_DATE",
    "RECR_INFO_SWA_JOB_ORDER_END": "RECR_INFO_JOB_END_DATE",

    "RECR_INFO_SUNDAY_NEWSPAPER": "RECR_INFO_IS_NEWSPAPER_SUNDAY",
    "RI_1ST_AD_NEWSPAPER_NAME": "RECR_INFO_NEWSPAPER_NAME",
    "RECR_INFO_FIRST_AD_START": "RECR_INFO_AD_DATE1",
    "RI_2ND_AD_NEWSPAPER_NAME": "RECR_INFO_NEWSPAPER_NAME2",
    "RI_2ND_AD_NEWSPAPER_OR_JOURNAL": "RECR_INFO_RECRUIT_AD_TYPE",
    "RECR_INFO_SECOND_AD_START": "RECR_INFO_AD_DATE2",

    "RECR_INFO_JOB_FAIR_FROM": "RECR_OCC_JOB_FAIR_FROM",
    "RECR_INFO_JOB_FAIR_TO": "RECR_OCC_JOB_FAIR_TO",
    "RECR_INFO_ON_CAMPUS_RECR_FROM": "RECR_OCC_ON_CAMPUS_FROM",
    "RECR_INFO_ON_CAMPUS_RECR_TO": "RECR_OCC_ON_CAMPUS_TO",

    "RI_EMPLOYER_WEB_POST_FROM": "RECR_OCC_EMP_WEBSITE_FROM",
    "RI_EMPLOYER_WEB_POST_TO": "RECR_OCC_EMP_WEBSITE_TO",
    "RECR_INFO_PRO_ORG_ADVERT_FROM": "RECR_OCC_TRADE_ORG_FROM",
    "RECR_INFO_PRO_ORG_ADVERT_TO": "RECR_OCC_TRADE_ORG_TO",
    "RI_JOB_SEARCH_WEBSITE_FROM": "RECR_OCC_JOB_SEARCH_FROM",
    "RI_JOB_SEARCH_WEBSITE_TO": "RECR_OCC_JOB_SEARCH_TO",
    "RI_PVT_EMPLOYMENT_FIRM_FROM": "RECR_OCC_PRIVATE_EMP_FROM",
    "RI_PVT_EMPLOYMENT_FIRM_TO": "RECR_OCC_PRIVATE_EMP_TO",
    "RI_EMPLOYEE_REFERRAL_PROG_FROM": "RECR_OCC_EMP_REFERRAL_FROM",
    "RI_EMPLOYEE_REFERRAL_PROG_TO": "RECR_OCC_EMP_REFERRAL_TO",
    "RI_CAMPUS_PLACEMENT_FROM": "RECR_OCC_CAMPUS_PLACEMENT_FROM",
    "RI_CAMPUS_PLACEMENT_TO": "RECR_OCC_CAMPUS_PLACEMENT_TO",
    "RI_LOCAL_ETHNIC_PAPER_FROM": "RECR_OCC_LOCAL_NEWSPAPER_FROM",
    "RI_LOCAL_ETHNIC_PAPER_TO": "RECR_OCC_LOCAL_NEWSPAPER_TO",

    "RECR_INFO_RADIO_TV_AD_FROM": "RECR_OCC_RADIO_AD_FROM",
    "RECR_INFO_RADIO_TV_AD_TO": "RECR_OCC_RADIO_AD_TO",

    "RECR_INFO_EMPLOYER_REC_PAYMENT": "OTHER_REQ_EMP_REC_PAYMENT",
    "RECR_INFO_BARG_REP_NOTIFIED": "NOTICE_POST_BARGAIN_REP",
    "RI_POSTED_NOTICE_AT_WORKSITE": "NOTICE_POST_BARGAIN_REP_PHYSICAL",
    "RI_LAYOFF_IN_PAST_SIX_MONTHS": "OTHER_REQ_EMP_LAYOFF",
    "RI_US_WORKERS_CONSIDERED": None,

    "FOREIGN_WORKER_INFO_CITY": None,
    "FOREIGN_WORKER_INFO_STATE": None,
    "FW_INFO_POSTAL_CODE": None,
    "COUNTRY_OF_CITIZENSHIP": None,
    "FW_INFO_BIRTH_COUNTRY": None,
    "CLASS_OF_ADMISSION": None,

    "FOREIGN_WORKER_INFO_EDUCATION": None,
    "FW_INFO_EDUCATION_OTHER": None,
    "FOREIGN_WORKER_INFO_MAJOR": None,
    "FW_INFO_YR_REL_EDU_COMPLETED": None,
    "FOREIGN_WORKER_INFO_INST": None,

    "FW_INFO_TRAINING_COMP": None,
    "FW_INFO_REQ_EXPERIENCE": None,
    "FW_INFO_ALT_EDU_EXPERIENCE": None,
    "FW_INFO_REL_OCCUP_EXP": None,

    "PREPARER_INFO_EMP_COMPLETED": None,
    "PREPARER_INFO_TITLE": None,
    "EMPLOYER_DECL_INFO_TITLE": None,

    "NAICS_US_CODE": "EMP_NAICS",
    "NAICS_US_TITLE": None,
    "PW_JOB_TITLE": "JOB_TITLE",
}
MAPPINGS["2018"] = {
    "CASE_NUMBER": "CASE_NUMBER",  # direct match
    "DECISION_DATE": "DECISION_DATE",  # direct match
    "CASE_STATUS": "CASE_STATUS",  # direct match
    "CASE_RECEIVED_DATE": "RECEIVED_DATE",  # best semantic match
    "REFILE": None,
    "ORIG_FILE_DATE": None,
    "ORIG_CASE_NO": None,
    "SCHD_A_SHEEPHERDER": None,

    "EMPLOYER_NAME": "EMP_BUSINESS_NAME",
    "EMPLOYER_ADDRESS_1": "EMP_ADDR1",
    "EMPLOYER_ADDRESS_2": "EMP_ADDR2",
    "EMPLOYER_CITY": "EMP_CITY",
    "EMPLOYER_STATE": "EMP_STATE",
    "EMPLOYER_COUNTRY": "EMP_COUNTRY",
    "EMPLOYER_POSTAL_CODE": "EMP_POSTCODE",
    "EMPLOYER_PHONE": "EMP_PHONE",
    "EMPLOYER_PHONE_EXT": "EMP_PHONEEXT",
    "EMPLOYER_NUM_EMPLOYEES": "EMP_NUM_PAYROLL",
    "EMPLOYER_YR_ESTAB": "EMP_YEAR_COMMENCED",

    "FW_OWNERSHIP_INTEREST": "EMP_WORKER_INTEREST",  # ownership -> EMP_WORKER_INTEREST only

    "AGENT_FIRM_NAME": "ATTY_AG_LAW_FIRM_NAME",
    "AGENT_CITY": "ATTY_AG_CITY",
    "AGENT_STATE": "ATTY_AG_STATE",

    "PW_TRACK_NUM": None,
    "PW_SOC_CODE": None,
    "PW_SOC_TITLE": None,
    "PW_LEVEL_9089": None,
    "PW_AMOUNT_9089": None,
    "PW_UNIT_OF_PAY_9089": None,
    "PW_SOURCE_NAME_9089": None,
    "PW_SOURCE_NAME_OTHER_9089": None,
    "PW_DETERM_DATE": None,
    "PW_EXPIRE_DATE": None,

    "WAGE_OFFER_FROM_9089": "JOB_OPP_WAGE_FROM",
    "WAGE_OFFER_TO_9089": "JOB_OPP_WAGE_TO",
    "WAGE_OFFER_UNIT_OF_PAY_9089": "JOB_OPP_WAGE_PER",

    "JOB_INFO_WORK_CITY": "PRIMARY_WORKSITE_CITY",
    "JOB_INFO_WORK_STATE": "PRIMARY_WORKSITE_STATE",
    "JOB_INFO_WORK_POSTAL_CODE": "PRIMARY_WORKSITE_POSTAL_CODE",
    "JOB_INFO_JOB_TITLE": "JOB_TITLE",

    "JOB_INFO_EDUCATION": None,
    "JOB_INFO_EDUCATION_OTHER": None,
    "JOB_INFO_MAJOR": None,
    "JOB_INFO_TRAINING": None,
    "JOB_INFO_TRAINING_NUM_MONTHS": None,
    "JOB_INFO_TRAINING_FIELD": None,
    "JOB_INFO_EXPERIENCE": None,
    "JOB_INFO_EXPERIENCE_NUM_MONTHS": None,
    "JOB_INFO_ALT_FIELD": None,
    "JOB_INFO_ALT_FIELD_NAME": None,
    "JOB_INFO_ALT_COMBO_ED_EXP": None,
    "JOB_INFO_ALT_COMBO_ED": None,
    "JOB_INFO_ALT_COMBO_ED_OTHER": None,
    "JOB_INFO_ALT_CMB_ED_OTH_YRS": None,
    "JOB_INFO_FOREIGN_ED": None,
    "JOB_INFO_ALT_OCC": None,
    "JOB_INFO_ALT_OCC_NUM_MONTHS": None,
    "JOB_INFO_ALT_OCC_JOB_TITLE": None,
    "JOB_INFO_JOB_REQ_NORMAL": None,
    "JOB_INFO_FOREIGN_LANG_REQ": None,
    "JOB_INFO_COMBO_OCCUPATION": None,

    "JI_OFFERED_TO_SEC_J_FW": None,
    "JI_FW_LIVE_ON_PREMISES": "OTHER_REQ_JOB_EMP_PREMISES",
    "JI_LIVE_IN_DOMESTIC_SERVICE": "OTHER_REQ_IS_LIVEIN_HOUSEHOLD",
    "JI_LIVE_IN_DOM_SVC_CONTRACT": "OTHER_REQ_IS_EMP_PROVIDED_CONT",

    "RECR_INFO_PROFESSIONAL_OCC": None,
    "RECR_INFO_COLL_UNIV_TEACHER": None,
    "RECR_INFO_COLL_TEACH_COMP_PROC": None,
    "RI_COLL_TCH_BASIC_PROCESS": None,
    "RI_COLL_TEACH_SELECT_DATE": None,
    "RI_COLL_TEACH_PRO_JNL": None,

    "RECR_INFO_SWA_JOB_ORDER_START": "RECR_INFO_JOB_START_DATE",
    "RECR_INFO_SWA_JOB_ORDER_END": "RECR_INFO_JOB_END_DATE",

    "RECR_INFO_SUNDAY_NEWSPAPER": "RECR_INFO_IS_NEWSPAPER_SUNDAY",
    "RI_1ST_AD_NEWSPAPER_NAME": "RECR_INFO_NEWSPAPER_NAME",
    "RECR_INFO_FIRST_AD_START": "RECR_INFO_AD_DATE1",
    "RI_2ND_AD_NEWSPAPER_NAME": "RECR_INFO_NEWSPAPER_NAME2",
    "RI_2ND_AD_NEWSPAPER_OR_JOURNAL": "RECR_INFO_RECRUIT_AD_TYPE",
    "RECR_INFO_SECOND_AD_START": "RECR_INFO_AD_DATE2",

    "RECR_INFO_JOB_FAIR_FROM": "RECR_OCC_JOB_FAIR_FROM",
    "RECR_INFO_JOB_FAIR_TO": "RECR_OCC_JOB_FAIR_TO",
    "RECR_INFO_ON_CAMPUS_RECR_FROM": "RECR_OCC_ON_CAMPUS_FROM",
    "RECR_INFO_ON_CAMPUS_RECR_TO": "RECR_OCC_ON_CAMPUS_TO",

    "RI_EMPLOYER_WEB_POST_FROM": "RECR_OCC_EMP_WEBSITE_FROM",
    "RI_EMPLOYER_WEB_POST_TO": "RECR_OCC_EMP_WEBSITE_TO",
    "RECR_INFO_PRO_ORG_ADVERT_FROM": "RECR_OCC_TRADE_ORG_FROM",
    "RECR_INFO_PRO_ORG_ADVERT_TO": "RECR_OCC_TRADE_ORG_TO",
    "RI_JOB_SEARCH_WEBSITE_FROM": "RECR_OCC_JOB_SEARCH_FROM",
    "RI_JOB_SEARCH_WEBSITE_TO": "RECR_OCC_JOB_SEARCH_TO",
    "RI_PVT_EMPLOYMENT_FIRM_FROM": "RECR_OCC_PRIVATE_EMP_FROM",
    "RI_PVT_EMPLOYMENT_FIRM_TO": "RECR_OCC_PRIVATE_EMP_TO",
    "RI_EMPLOYEE_REFERRAL_PROG_FROM": "RECR_OCC_EMP_REFERRAL_FROM",
    "RI_EMPLOYEE_REFERRAL_PROG_TO": "RECR_OCC_EMP_REFERRAL_TO",
    "RI_CAMPUS_PLACEMENT_FROM": "RECR_OCC_CAMPUS_PLACEMENT_FROM",
    "RI_CAMPUS_PLACEMENT_TO": "RECR_OCC_CAMPUS_PLACEMENT_TO",
    "RI_LOCAL_ETHNIC_PAPER_FROM": "RECR_OCC_LOCAL_NEWSPAPER_FROM",
    "RI_LOCAL_ETHNIC_PAPER_TO": "RECR_OCC_LOCAL_NEWSPAPER_TO",

    "RECR_INFO_RADIO_TV_AD_FROM": "RECR_OCC_RADIO_AD_FROM",
    "RECR_INFO_RADIO_TV_AD_TO": "RECR_OCC_RADIO_AD_TO",

    "RECR_INFO_EMPLOYER_REC_PAYMENT": "OTHER_REQ_EMP_REC_PAYMENT",
    "RECR_INFO_BARG_REP_NOTIFIED": "NOTICE_POST_BARGAIN_REP",
    "RI_POSTED_NOTICE_AT_WORKSITE": "NOTICE_POST_BARGAIN_REP_PHYSICAL",
    "RI_LAYOFF_IN_PAST_SIX_MONTHS": "OTHER_REQ_EMP_LAYOFF",
    "RI_US_WORKERS_CONSIDERED": None,

    "FOREIGN_WORKER_INFO_CITY": None,
    "FOREIGN_WORKER_INFO_STATE": None,
    "FW_INFO_POSTAL_CODE": None,
    "COUNTRY_OF_CITIZENSHIP": None,
    "FW_INFO_BIRTH_COUNTRY": None,
    "CLASS_OF_ADMISSION": None,

    "FOREIGN_WORKER_INFO_EDUCATION": None,
    "FW_INFO_EDUCATION_OTHER": None,
    "FOREIGN_WORKER_INFO_MAJOR": None,
    "FW_INFO_YR_REL_EDU_COMPLETED": None,
    "FOREIGN_WORKER_INFO_INST": None,

    "FW_INFO_TRAINING_COMP": None,
    "FW_INFO_REQ_EXPERIENCE": None,
    "FW_INFO_ALT_EDU_EXPERIENCE": None,
    "FW_INFO_REL_OCCUP_EXP": None,

    "PREPARER_INFO_EMP_COMPLETED": None,
    "PREPARER_INFO_TITLE": None,
    "EMPLOYER_DECL_INFO_TITLE": None,

    "NAICS_US_CODE": "EMP_NAICS",
    "NAICS_US_TITLE": None,
    "PW_JOB_TITLE_9089": "JOB_TITLE",
}
MAPPINGS["2017"] = MAPPINGS["2018"]
MAPPINGS["2016"] = MAPPINGS["2018"]
MAPPINGS["2015"] = {
    "CASE_NUMBER": "CASE_NUMBER",  # direct match
    "DECISION_DATE": "DECISION_DATE",  # direct match
    "CASE_STATUS": "CASE_STATUS",  # direct match
    "CASE_RECEIVED_DATE": "RECEIVED_DATE",  # best semantic match
    "REFILE": None,
    "ORIG_FILE_DATE": None,
    "ORIG_CASE_NO": None,
    "SCHD_A_SHEEPHERDER": None,

    "EMPLOYER_NAME": "EMP_BUSINESS_NAME",
    "EMPLOYER_ADDRESS_1": "EMP_ADDR1",
    "EMPLOYER_ADDRESS_2": "EMP_ADDR2",
    "EMPLOYER_CITY": "EMP_CITY",
    "EMPLOYER_STATE": "EMP_STATE",
    "EMPLOYER_COUNTRY": "EMP_COUNTRY",
    "EMPLOYER_POSTAL_CODE": "EMP_POSTCODE",
    "EMPLOYER_PHONE": "EMP_PHONE",
    "EMPLOYER_PHONE_EXT": "EMP_PHONEEXT",
    "EMPLOYER_NUM_EMPLOYEES": "EMP_NUM_PAYROLL",
    "EMPLOYER_YR_ESTAB": "EMP_YEAR_COMMENCED",

    "FW_OWNERSHIP_INTEREST": "EMP_WORKER_INTEREST",  # ownership -> EMP_WORKER_INTEREST only

    "AGENT_FIRM_NAME": "ATTY_AG_LAW_FIRM_NAME",
    "AGENT_CITY": "ATTY_AG_CITY",
    "AGENT_STATE": "ATTY_AG_STATE",

    "PW_TRACK_NUM": None,
    "PW_SOC_CODE": None,
    "PW_SOC_TITLE": None,
    "PW_LEVEL_9089": None,
    "PW_AMOUNT_9089": None,
    "PW_UNIT_OF_PAY_9089": None,
    "PW_SOURCE_NAME_9089": None,
    "PW_SOURCE_NAME_OTHER_9089": None,
    "PW_DETERM_DATE": None,
    "PW_EXPIRE_DATE": None,

    "WAGE_OFFER_FROM_9089": "JOB_OPP_WAGE_FROM",
    "WAGE_OFFER_TO_9089": "JOB_OPP_WAGE_TO",
    "WAGE_OFFER_UNIT_OF_PAY_9089": "JOB_OPP_WAGE_PER",

    "JOB_INFO_WORK_CITY": "PRIMARY_WORKSITE_CITY",
    "JOB_INFO_WORK_STATE": "PRIMARY_WORKSITE_STATE",
    "JOB_INFO_WORK_POSTAL_CODE": "PRIMARY_WORKSITE_POSTAL_CODE",
    "JOB_INFO_JOB_TITLE": "JOB_TITLE",

    "JOB_INFO_EDUCATION": None,
    "JOB_INFO_EDUCATION_OTHER": None,
    "JOB_INFO_MAJOR": None,
    "JOB_INFO_TRAINING": None,
    "JOB_INFO_TRAINING_NUM_MONTHS": None,
    "JOB_INFO_TRAINING_FIELD": None,
    "JOB_INFO_EXPERIENCE": None,
    "JOB_INFO_EXPERIENCE_NUM_MONTHS": None,
    "JOB_INFO_ALT_FIELD": None,
    "JOB_INFO_ALT_FIELD_NAME": None,
    "JOB_INFO_ALT_COMBO_ED_EXP": None,
    "JOB_INFO_ALT_COMBO_ED": None,
    "JOB_INFO_ALT_COMBO_ED_OTHER": None,
    "JOB_INFO_ALT_CMB_ED_OTH_YRS": None,
    "JOB_INFO_FOREIGN_ED": None,
    "JOB_INFO_ALT_OCC": None,
    "JOB_INFO_ALT_OCC_NUM_MONTHS": None,
    "JOB_INFO_ALT_OCC_JOB_TITLE": None,
    "JOB_INFO_JOB_REQ_NORMAL": None,
    "JOB_INFO_FOREIGN_LANG_REQ": None,
    "JOB_INFO_COMBO_OCCUPATION": None,

    "JI_OFFERED_TO_SEC_J_FW": None,
    "JI_FW_LIVE_ON_PREMISES": "OTHER_REQ_JOB_EMP_PREMISES",
    "JI_LIVE_IN_DOMESTIC_SERVICE": "OTHER_REQ_IS_LIVEIN_HOUSEHOLD",
    "JI_LIVE_IN_DOM_SVC_CONTRACT": "OTHER_REQ_IS_EMP_PROVIDED_CONT",

    "RECR_INFO_PROFESSIONAL_OCC": None,
    "RECR_INFO_COLL_UNIV_TEACHER": None,
    "RECR_INFO_COLL_TEACH_COMP_PROC": None,
    "RI_COLL_TCH_BASIC_PROCESS": None,
    "RI_COLL_TEACH_SELECT_DATE": None,
    "RI_COLL_TEACH_PRO_JNL": None,

    "RECR_INFO_SWA_JOB_ORDER_START": "RECR_INFO_JOB_START_DATE",
    "RECR_INFO_SWA_JOB_ORDER_END": "RECR_INFO_JOB_END_DATE",

    "RECR_INFO_SUNDAY_NEWSPAPER": "RECR_INFO_IS_NEWSPAPER_SUNDAY",
    "RI_1ST_AD_NEWSPAPER_NAME": "RECR_INFO_NEWSPAPER_NAME",
    "RECR_INFO_FIRST_AD_START": "RECR_INFO_AD_DATE1",
    "RI_2ND_AD_NEWSPAPER_NAME": "RECR_INFO_NEWSPAPER_NAME2",
    "RI_2ND_AD_NEWSPAPER_OR_JOURNAL": "RECR_INFO_RECRUIT_AD_TYPE",
    "RECR_INFO_SECOND_AD_START": "RECR_INFO_AD_DATE2",

    "RECR_INFO_JOB_FAIR_FROM": "RECR_OCC_JOB_FAIR_FROM",
    "RECR_INFO_JOB_FAIR_TO": "RECR_OCC_JOB_FAIR_TO",
    "RECR_INFO_ON_CAMPUS_RECR_FROM": "RECR_OCC_ON_CAMPUS_FROM",
    "RECR_INFO_ON_CAMPUS_RECR_TO": "RECR_OCC_ON_CAMPUS_TO",

    "RI_EMPLOYER_WEB_POST_FROM": "RECR_OCC_EMP_WEBSITE_FROM",
    "RI_EMPLOYER_WEB_POST_TO": "RECR_OCC_EMP_WEBSITE_TO",
    "RECR_INFO_PRO_ORG_ADVERT_FROM": "RECR_OCC_TRADE_ORG_FROM",
    "RECR_INFO_PRO_ORG_ADVERT_TO": "RECR_OCC_TRADE_ORG_TO",
    "RI_JOB_SEARCH_WEBSITE_FROM": "RECR_OCC_JOB_SEARCH_FROM",
    "RI_JOB_SEARCH_WEBSITE_TO": "RECR_OCC_JOB_SEARCH_TO",
    "RI_PVT_EMPLOYMENT_FIRM_FROM": "RECR_OCC_PRIVATE_EMP_FROM",
    "RI_PVT_EMPLOYMENT_FIRM_TO": "RECR_OCC_PRIVATE_EMP_TO",
    "RI_EMPLOYEE_REFERRAL_PROG_FROM": "RECR_OCC_EMP_REFERRAL_FROM",
    "RI_EMPLOYEE_REFERRAL_PROG_TO": "RECR_OCC_EMP_REFERRAL_TO",
    "RI_CAMPUS_PLACEMENT_FROM": "RECR_OCC_CAMPUS_PLACEMENT_FROM",
    "RI_CAMPUS_PLACEMENT_TO": "RECR_OCC_CAMPUS_PLACEMENT_TO",
    "RI_LOCAL_ETHNIC_PAPER_FROM": "RECR_OCC_LOCAL_NEWSPAPER_FROM",
    "RI_LOCAL_ETHNIC_PAPER_TO": "RECR_OCC_LOCAL_NEWSPAPER_TO",

    "RECR_INFO_RADIO_TV_AD_FROM": "RECR_OCC_RADIO_AD_FROM",
    "RECR_INFO_RADIO_TV_AD_TO": "RECR_OCC_RADIO_AD_TO",

    "RECR_INFO_EMPLOYER_REC_PAYMENT": "OTHER_REQ_EMP_REC_PAYMENT",
    "RECR_INFO_BARG_REP_NOTIFIED": "NOTICE_POST_BARGAIN_REP",
    "RI_POSTED_NOTICE_AT_WORKSITE": "NOTICE_POST_BARGAIN_REP_PHYSICAL",
    "RI_LAYOFF_IN_PAST_SIX_MONTHS": "OTHER_REQ_EMP_LAYOFF",
    "RI_US_WORKERS_CONSIDERED": None,

    "FOREIGN_WORKER_INFO_CITY": None,
    "FOREIGN_WORKER_INFO_STATE": None,
    "FOREIGN_WORKER_INFO_POSTAL_CODE": None, # FW_INFO_POSTAL_CODE is what is writes as in 2018-2016
    "COUNTRY_OF_CITIZENSHIP": None,
    "FOREIGN_WORKER_INFO_BIRTH_COUNTRY": None, # FW_INFO_BIRTH_COUNTRY is what is writes as in 2018-2016
    "CLASS_OF_ADMISSION": None,

    "FOREIGN_WORKER_INFO_EDUCATION": None,
    "FOREIGN_WORKER_INFO_EDUCATION_OTHER": None, # same sentiment as above comments differ from 2018-2016
    "FOREIGN_WORKER_INFO_MAJOR": None,
    "FOREIGN_WORKER_INFO_YR_REL_EDU_COMPLETED": None, # same sentiment as above comments differ from 2018-2016
    "FOREIGN_WORKER_INFO_INST": None,

    "FOREIGN_WORKER_INFO_TRAINING_COMP": None, # same sentiment as above comments differ from 2018-2016
    "FOREIGN_WORKER_INFO_REQ_EXPERIENCE": None, # same sentiment as above comments differ from 2018-2016
    "FOREIGN_WORKER_INFO_ALT_EDU_EXPERIENCE": None, # same sentiment as above comments differ from 2018-2016
    "FOREIGN_WORKER_INFO_REL_OCCUP_EXP": None, # same sentiment as above comments differ from 2018-2016

    "PREPARER_INFO_EMP_COMPLETED": None,
    "PREPARER_INFO_TITLE": None,
    "EMPLOYER_DECL_INFO_TITLE": None,

    "NAICS_US_CODE": "EMP_NAICS",
    "NAICS_US_TITLE": None,
    "PW_JOB_TITLE_9089": "JOB_TITLE",
}
MAPPINGS["2014"] = {

    "case_no": "CASE_NUMBER",
    "decision_date": "DECISION_DATE",
    "case_status": "CASE_STATUS",

    "application_type": None,

    "employer_name": "EMP_BUSINESS_NAME",
    "employer_address_1": "EMP_ADDR1",
    "employer_address_2": "EMP_ADDR2",
    "employer_city": "EMP_CITY",
    "employer_state": "EMP_STATE",
    "employer_postal_code": "EMP_POSTCODE",

    "2007_naics_us_code": "EMP_NAICS",
    "2007_naics_us_title": None,
    "us_economic_sector": None,

    "pw_soc_code": None,
    "pw_soc_title": None,
    "pw_job_title_9089": "JOB_TITLE",
    "pw_level_9089": None,

    "pw_source_name_9089": None,
    "pw_amount_9089": None,
    "pw_unit_of_pay_9089": None,

    "wage_offer_from_9089": "JOB_OPP_WAGE_FROM",
    "wage_offer_to_9089": "JOB_OPP_WAGE_TO",
    "wage_offer_unit_of_pay_9089": "JOB_OPP_WAGE_PER",

    "job_info_work_city": "PRIMARY_WORKSITE_CITY",
    "job_info_work_state": "PRIMARY_WORKSITE_STATE",

    "country_of_citzenship": None,
    "class_of_admission": None,
}
MAPPINGS["2013"] = MAPPINGS["2014"]
MAPPINGS["2012"] = MAPPINGS["2014"]
MAPPINGS["2011"] = MAPPINGS["2014"]
MAPPINGS["2010"] = MAPPINGS["2014"]
MAPPINGS["2009"] = {
    # 2009 likes the spaces lol
    "case_number": "CASE_NUMBER",
    "decision date": "DECISION_DATE",
    "case status": "CASE_STATUS",

    "application type": None,

    "employer name": "EMP_BUSINESS_NAME",
    "employer address_1": "EMP_ADDR1",
    "employer address_2": "EMP_ADDR2",
    "employer city": "EMP_CITY",
    "employer state": "EMP_STATE",
    "employer postal_code": "EMP_POSTCODE",

    "2007 naics us code": "EMP_NAICS",
    "2007 naics us title": None,
    "us economic sector": None,

    "pw soc code": None,
    "pw soc title": None,
    "pw job title 9089": "JOB_TITLE",
    "pw level 9089": None,

    "pw source_name_9089": None,
    "pw amount_9089": None,
    "pw unit of pay 9089": None,

    "wage offer from 9089": "JOB_OPP_WAGE_FROM",
    "wage offer to 9089": "JOB_OPP_WAGE_TO",
    "wage offer unit of pay 9089": "JOB_OPP_WAGE_PER",

    "job info work city": "PRIMARY_WORKSITE_CITY",
    "job info work state": "PRIMARY_WORKSITE_STATE",

    "country of citzenship": None,
    "class of admission": None,
}
MAPPINGS["2008"] = {

    "case_number": "CASE_NUMBER", # same as 2014 except this line is case_number
    "decision_date": "DECISION_DATE",
    "case_status": "CASE_STATUS",

    "application_type": None,

    "employer_name": "EMP_BUSINESS_NAME",
    "employer_address_1": "EMP_ADDR1",
    "employer_address_2": "EMP_ADDR2",
    "employer_city": "EMP_CITY",
    "employer_state": "EMP_STATE",
    "employer_postal_code": "EMP_POSTCODE",

    "2007_naics_us_code": "EMP_NAICS",
    "2007_naics_us_title": None,
    "us_economic_sector": None,

    "pw_soc_code": None,
    "pw_soc_title": None,
    "pw_job_title_9089": "JOB_TITLE",
    "pw_level_9089": None,

    "pw_source_name_9089": None,
    "pw_amount_9089": None,
    "pw_unit_of_pay_9089": None,

    "wage_offer_from_9089": "JOB_OPP_WAGE_FROM",
    "wage_offer_to_9089": "JOB_OPP_WAGE_TO",
    "wage_offer_unit_of_pay_9089": "JOB_OPP_WAGE_PER",

    "job_info_work_city": "PRIMARY_WORKSITE_CITY",
    "job_info_work_state": "PRIMARY_WORKSITE_STATE",

    "country_of_citzenship": None,
    "class_of_admission": None,
}
# ---------------------------------------------------------
# HELPER FUNCTIONS
# ---------------------------------------------------------
def normalize_columns(cols):
    """Lowercase, replace spaces/dashes with underscores."""
    return cols.str.strip().str.lower().str.replace(" ", "_").str.replace("-", "_")

def detect_mapping_key(year, filename):
    filename = filename.lower()
    if year in [2024, 2025]:
        if "new_form" in filename:
            return f"{year}_new"
        else:
            return f"{year}_old"
    return str(year)

def ensure_final_schema(df):
    """Fast: add missing FINAL_SCHEMA columns via one concat, preventing fragmentation."""
    missing = [c for c in FINAL_SCHEMA if c not in df.columns]
    
    if missing:
        # Create a new frame for missing columns
        missing_df = pd.DataFrame({c: pd.NA for c in missing}, index=df.index)

        # Join at once
        df = pd.concat([df, missing_df], axis=1)

    # Reorder to final schema
    return df[FINAL_SCHEMA]


# ---------------------------------------------------------
# MAIN PROCESSING
# ---------------------------------------------------------
def compile_perm():
    logger.info("="*80)
    logger.info("PERM Data Compilation")
    logger.info("="*80)
    
    all_dfs = []
    
    for year_dir in sorted(os.listdir(DATA_PATH)):
        year_path = os.path.join(DATA_PATH, year_dir)
        if not os.path.isdir(year_path):
            continue
        
        try:
            year = int(year_dir)
        except ValueError:
            continue
        
        logger.info(f"\n--- Year {year} ---")
        
        for fname in os.listdir(year_path):
            if not fname.endswith(".parquet"):
                continue
            
            parquet_path = os.path.join(year_path, fname)
            logger.info(f"Loading: {fname}")
            
            try:
                # Load parquet
                df = pd.read_parquet(parquet_path)
                logger.info(f"  Rows: {len(df):,}")
                
                # Normalize columns
                df.columns = normalize_columns(df.columns)
                
                # Determine mapping
                mapping_key = detect_mapping_key(year, fname)
                mapping = MAPPINGS.get(mapping_key, {})
                
                if not mapping:
                    logger.warning(f"  No mapping for '{mapping_key}', skipping")
                    continue
                
                # Rename columns
                df = df.rename(columns=mapping)
                
                # Add metadata
                df["YEAR"] = year
                df["FORM_TYPE"] = mapping_key
                
                # Ensure final schema
                df = ensure_final_schema(df)
                
                all_dfs.append(df)
                logger.info(f"Processed :)")
                
            except Exception as e:
                logger.error(f"Error: {e}")
                continue
    
    if not all_dfs:
        logger.error("No data processed!")
        return
    
    # Combine
    logger.info("\nCombining all data...")
    final = pd.concat(all_dfs, ignore_index=True)
    
    # Dedupe
    logger.info("Deduplicating...")
    final = final.drop_duplicates(subset=["CASE_NUMBER"], keep="first")
    
    logger.info(f"\nFinal: {len(final):,} rows")
    
    # Save
    output = os.path.join(PROJECT_ROOT, "perm_db.csv")
    final.to_csv(output, index=False)
    logger.info(f"Saved: {output}")
    logger.info("="*80)

if __name__ == "__main__":
    compile_perm()