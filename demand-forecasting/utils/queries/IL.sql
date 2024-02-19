select replace(label_print_year_week_utc,'-W','') as year_week, count(*) from prod_PRESENTATION_INTERNAL_DB.GENERAL_SCHEMA.GENERAL_DATASET_1
--where label_print_date_utc >= date(current_timestamp) - 365
where is_physical_shipment = 1
and consolidation_state = 'IL'
and current_milestone_id >300
and is_return = 0
group by 1
order by 1 desc