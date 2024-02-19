from utils.extract_snowflake_data import snowflake_extract


consolidation_states = ['CA', 'IL', 'NJ']

for i in consolidation_states:
    snowflake_extract(i)

