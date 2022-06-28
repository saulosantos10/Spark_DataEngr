import sqlalchemy
from sqlalchemy import create_engine
import pandas as pd

def insert(db_url, db_table, df):
    if not df.empty:
        db_connection = create_engine(db_url)
        df.to_sql(con=db_connection, name=db_table, if_exists='append', index=False)

def query(db_url, query):
    db_connection = create_engine(db_url)
    df = pd.read_sql(query, db_connection)
    return df
