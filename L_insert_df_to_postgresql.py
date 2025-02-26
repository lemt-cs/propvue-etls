def insert_to_postgresql(schema, table, df, replace_or_append):
    import pandas as pd
    from sqlalchemy import create_engine
    from dotenv import load_dotenv
    import os

    load_dotenv(dotenv_path=r"/home/lemuel-torrefiel/airflow/dags/.env")
    conn_string = os.getenv("POSTGRES_CON")
    engine = create_engine(conn_string)

    print(f"writing df to postgresql")
    df.to_sql(table, engine, schema, if_exists=replace_or_append, chunksize=10000)
    print(f"Done writing")
    