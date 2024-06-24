import os
import sys
from datetime import datetime
from datetime import timedelta
import pandas as pd
from sqlalchemy import create_engine, text
import warnings

# override print so each statement is timestamped
old_print = print
def timestamped_print(*args, **kwargs):
  old_print(datetime.now(), *args, **kwargs)
print = timestamped_print


def slicer(my_str,sub):
        index=my_str.find(sub)
        if index !=-1 :
            return my_str[index:] 
        else :
            raise Exception('Sub string not found!')
        
        
def postgres_upsert(table, conn, keys, data_iter):
    from sqlalchemy.dialects.postgresql import insert

    data = [dict(zip(keys, row)) for row in data_iter]

    insert_statement = insert(table.table).values(data)
    upsert_statement = insert_statement.on_conflict_do_update(
        constraint=f"{table.table.name}_pkey",
        set_={c.key: c for c in insert_statement.excluded},
    )
    conn.execute(upsert_statement)
    
    
def postgres_safe_insert(table, conn, keys, data_iter):
    from sqlalchemy.dialects.postgresql import insert

    data = [dict(zip(keys, row)) for row in data_iter]

    insert_statement = insert(table.table).values(data)
    upsert_statement = insert_statement.on_conflict_do_nothing(
        constraint=f"{table.table.name}_pkey"
    )
    conn.execute(upsert_statement)

def main():
    print("Entering main of timestamp-fix.py")
    
    # from env_vars import set_env_vars
    # set_env_vars()
    
    ########################
    # Establish DB engine  #
    ########################

    SQLALCHEMY_DATABASE_URL = "postgresql://" + os.environ.get('POSTGRESQL_USER') + ":" + os.environ.get(
        'POSTGRESQL_PASSWORD') + "@" + os.environ.get('POSTGRESQL_HOSTNAME') + "/" + os.environ.get('POSTGRESQL_DATABASE')

    engine = create_engine(SQLALCHEMY_DATABASE_URL)

    start_date = '2023-07-06 13:30:00-04:00'
    end_date = '2023-10-13 19:00:00-04:00'
    # end_date = '2023-07-08 19:00:00-04:00'

    print("QUERYING DATA")
    query = f"SELECT * FROM sensor_data WHERE \"sensor_ID\"='CB_02' AND date >= '{start_date}' AND date <= '{end_date}' ORDER BY date"
    data = pd.read_sql_query(query, engine)
    data = data.assign(date=data["date"] + pd.Timedelta(hours=1))
    data = data.assign(processed=False)
    data = data.set_index(["place", "sensor_ID", "date"])
    
    print("DELETING DATA")
    with engine.connect() as connection:
        result = connection.execute(text(f"DELETE FROM sensor_data WHERE \"sensor_ID\"='CB_02' AND date >= '{start_date}' AND date <= '{end_date}'"))

    print("WRITING UPDATED DATA")
    # try:
    data.to_sql("sensor_data", engine, if_exists = "append", method=postgres_upsert, chunksize = 3000)
    # except:
    #     warnings.warn("ERROR")

if __name__ == "__main__":
    main()