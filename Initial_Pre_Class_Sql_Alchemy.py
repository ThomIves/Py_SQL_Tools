import pandas as pd
import sqlalchemy
from sqlalchemy.engine import URL

# connection parameters
driver='{SQL Server}'
server='localhost\SQLEXPRESS'
database='master'
trusted_connection='yes'

# pyodbc connection string
conn_str = f'DRIVER={driver};SERVER={server};DATABASE={database};'
conn_str += f'TRUSTED_CONNECTION={trusted_connection}'

# create sqlalchemy engine connection URL
connection_url = URL.create(
    "mssql+pyodbc", query={"odbc_connect": conn_str})

engine = sqlalchemy.create_engine(connection_url)

d = {'value_1': [1, 2], 'value_2': [3, 4]}
df = pd.DataFrame(data=d)      

try:
    cs = """CREATE TABLE table_one
        (ident int IDENTITY(1,1) PRIMARY KEY,
        value_1 int NOT NULL,
        value_2 int NOT NULL)"""  
    with engine.connect() as conn:
        conn.execute(sqlalchemy.text(cs))
except sqlalchemy.exc.ProgrammingError:
    pass

df.to_sql('table_one', engine, if_exists="append", index=False)
qdf = pd.read_sql_query("SELECT * FROM table_one", engine)

print(qdf)
