import os
import json

from dbt_tableau.dbt_metadata_api import (
    dbt_get_account_id, dbt_get_projects, dbt_get_jobs, dbt_get_models_for_job,
    generate_dbt_exposures
)
from dbt_tableau.tableau import (
    authenticate_tableau, 
    tableau_get_databases, 
    tableau_get_downstream_workbooks, 
    merge_dbt_tableau_tables,
    get_tableau_columns,
    publish_tableau_column_descriptions
)
from dotenv import load_dotenv

load_dotenv()
#### dbt ####
API_BASE_URL = os.getenv("API_BASE_URL")
METADATA_API_URL = os.getenv("METADATA_API_URL")
DBT_API_KEY = os.getenv("DBT_API_PAT")
ACCOUNT_ID = dbt_get_account_id(API_BASE_URL,DBT_API_KEY)
#### Tableau ###
TABLEAU_SERVER =  os.getenv("TABLEAU_SERVER")
TABLEAU_SITE = os.getenv("TABLEAU_SITE")
TABLEAU_PAT_NAME = os.getenv("TABLEAU_PAT_NAME")
TABLEAU_PAT = os.getenv("TABLEAU_PAT")
TABLEAU_AUTH = authenticate_tableau(TABLEAU_SERVER, TABLEAU_SITE, TABLEAU_PAT_NAME, TABLEAU_PAT)

def restore_full_model_name(tableau_database_tables):
    for table in tableau_database_tables['tables']:
        if table['schema'].lower() == 'common':
            table['name'] = f"common_{table['name']}"
        elif table['schema'].lower() == 'core_ng':
            table['name'] = f"core_{table['name']}"
    return tableau_database_tables

if __name__ == "__main__":
    dbt_projects = dbt_get_projects(ACCOUNT_ID,API_BASE_URL,DBT_API_KEY)
    jobs = dbt_get_jobs(ACCOUNT_ID,API_BASE_URL,DBT_API_KEY, job_environment_ids=[1939])
    ### Returns metadata on all models
    models = dbt_get_models_for_job(METADATA_API_URL, DBT_API_KEY, 117857)
    reporting_models_metadata = []
    # for model in models:
    #     if "reporting" in model.get("schema"):
    #         print(f"Model: {model.get('name')}, dbt discovery API metadata: {model}")
    
    # Three production databases in tableau
    tableau_databases = tableau_get_databases(TABLEAU_SERVER, TABLEAU_AUTH)
    tableau_database_tables = restore_full_model_name(tableau_databases[1])
    merged_tables = merge_dbt_tableau_tables(tableau_databases[1], tableau_database_tables, models)
    common_search_cols = get_tableau_columns(TABLEAU_SERVER, merged_tables[48], TABLEAU_AUTH)
    publish_tableau_column_descriptions(TABLEAU_SERVER, merged_tables[48], common_search_cols, TABLEAU_AUTH)
    # for i,model in enumerate(merged_tables):
    #     print(f"Model {model.get('name')}, index: {i},")
    # all_downstream_workbooks = []
    # for table in merged_tables:
    #     downstream_workbooks = tableau_get_downstream_workbooks(TABLEAU_SERVER, table, TABLEAU_AUTH)
    #     all_downstream_workbooks.append(downstream_workbooks)
    # generate_dbt_exposures(all_downstream_workbooks, TABLEAU_SERVER, TABLEAU_SITE, 1)



"""
Model base_s3_supply_stress_forecast, index: 0,
Model booking_curve_evaluation, index: 1,
Model booking_curve_predictions, index: 2,
Model business_rule_forward_connect_occupancies_monitoring, index: 3,
Model business_rules_boost_impact, index: 4,
Model common_access_restrictions, index: 5,
Model common_adjustments, index: 6,
Model common_cancellations, index: 7,
Model common_car_instructions, index: 8,
Model common_car_stats, index: 9,
Model common_cars, index: 10,
Model common_checkins, index: 11,
Model common_cities_mapping, index: 12,
Model common_companies, index: 13,
Model common_dates, index: 14,
Model common_dbt_cloud_job_runs, index: 15,
Model common_dbt_cloud_job_test_coverages, index: 16,
Model common_dbt_cloud_model_test_coverages, index: 17,
Model common_dbt_job_durations, index: 18,
Model common_dbt_job_failure_percentages, index: 19,
Model common_dbt_job_failure_times, index: 20,
Model common_dbt_worst_model_performance_latest_run, index: 21,
Model common_devices_sessions_past_3_years, index: 22,
Model common_eu_supply_onboarding_offboarding_monthly_targets, index: 23,
Model common_extensions, index: 24,
Model common_face_verifications, index: 25,
Model common_geography_facts, index: 26,
Model common_global_backfill_occupancies_past_and_future_daily_enriched, index: 27,
Model common_last_checkouts, index: 28,
Model common_occupancies_daily, index: 29,
Model common_occupancies_past_and_future_daily_enriched, index: 30,
Model common_open_devices, index: 31,
Model common_open_installs_enriched, index: 32,
Model common_orders, index: 33,
Model common_owner_alerts, index: 34,
Model common_owner_segment_detailed, index: 35,
Model common_owner_segment_detailed_historized, index: 36,
Model common_owner_surveys, index: 37,
Model common_points_of_interest, index: 38,
Model common_price_recommendations, index: 39,
Model common_referrals, index: 40,
Model common_rental_credit_operations, index: 41,
Model common_rental_credit_spendings, index: 42,
Model common_rentals, index: 43,
Model common_rentals_and_adjustments, index: 44,
Model common_reporting_norway_cars_backfilled, index: 45,
Model common_reporting_norway_occupancies_backfilled, index: 46,
Model common_reporting_norway_rentals_backfilled, index: 47,
Model common_searches, index: 48,
Model common_searches_daily, index: 49,
Model common_sms, index: 50,
Model common_snowflake_warehouse_metering_xf, index: 51,
Model common_todos, index: 52,
Model common_user_fleet_manager_current, index: 53,
Model common_user_stats, index: 54,
Model common_users, index: 55,
Model common_zendesk_organizations, index: 56,
Model common_zendesk_tickets, index: 57,
Model common_zendesk_users, index: 58,
Model conversion_rate_monitoring_by_h3, index: 59,
Model core_car_history_use, index: 60,
Model core_car_versions, index: 61,
Model core_cars, index: 62,
Model core_companies, index: 63,
Model core_dates, index: 64,
Model core_devices_sessions_past_3_years, index: 65,
Model core_geography_facts, index: 66,
Model core_orders, index: 67,
Model core_revenue_aggregations_per_rental, index: 68,
Model core_users, index: 69,
Model degressivity_visits_and_refreshes, index: 70,
Model devices_sessions, index: 71,
Model experiments_details_apps, index: 72,
Model fallback_diagnostics_new_flow, index: 73,
Model fallback_diagnostics_validated_flow, index: 74,
Model finance_orders, index: 75,
Model finance_payments, index: 76,
Model gremlin_aggregated_by_agglomeration, index: 77,
Model gremlin_data, index: 78,
Model growth_rolling_sums_per_driver, index: 79,
Model growth_rolling_sums_per_driver_first_of_months_snapshots, index: 80,
Model interpolated_smart_pricing_monitoring, index: 81,
Model monitoring_by_h3, index: 82,
Model occupancies_pricing, index: 83,
Model prime_diagnostics_new_flow, index: 84,
Model prime_diagnostics_validated_flow, index: 85,
Model rentals_for_forward_dynamics, index: 86,
Model risk_access_restrictions, index: 87,
Model risk_access_restrictions_backlog, index: 88,
Model risk_charges, index: 89,
Model risk_face_verifications, index: 90,
Model risk_profile_verification_todos, index: 91,
Model risk_profile_verifications, index: 92,
Model risk_rangers_actions, index: 93,
Model risk_rangers_triggers, index: 94,
Model risk_rentals, index: 95,
Model searched_and_rented_days_pricing, index: 96,
Model smart_pricing_deactivation_comments, index: 97,
Model snowflake_warehouse_metering_xf, index: 98,
Model st_latest_price_updates, index: 99,
Model st_pricing_diagnostics, index: 100,
Model supply_stress_monitoring, index: 101,
Model temp_devices_sessions_add_channel, index: 102,
Model top_h3_performers, index: 103,
"""


