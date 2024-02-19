import snowflake.connector
import os
import pandas as pd
from datetime import datetime

from dotenv import load_dotenv

load_dotenv()


def get_query(path):
    with open(path, "r") as f:
        query_str = f.read()
    return query_str

def snowflake_extract(file) -> None:
    con = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
    )

    query = get_query(f"utils/queries/{file}.sql")

    cur = con.cursor()
    cur.execute(query)

    results = cur.fetchall()
    columns = [col[0] for col in cur.description]

    results_df = pd.DataFrame(data=results, columns=columns)

    file_name = f"{file}"

    results_df.to_csv(f"data/{file_name}.csv", index=False)

    cur.close()
    con.close()






# def snowflake_extract(data_ind: str) -> None:
#     con = snowflake.connector.connect(
#         user=os.getenv("SNOWFLAKE_USER"),
#         password=os.getenv("SNOWFLAKE_PASSWORD"),
#         account=os.getenv("SNOWFLAKE_ACCOUNT"),
#         warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
#         database=os.getenv("SNOWFLAKE_DATABASE"),
#         schema=os.getenv("SNOWFLAKE_SCHEMA"),
#     )

#     query = get_query(f"utils/queries/data.sql")

#     cur = con.cursor()
#     cur.execute(query)

#     results = cur.fetchall()
#     columns = [col[0] for col in cur.description]

#     results_df = pd.DataFrame(data=results, columns=columns)

#     file_name = f"data"

#     results_df.to_csv(f"data/{file_name}.csv", index=False)

#     cur.close()
#     con.close()
    
    