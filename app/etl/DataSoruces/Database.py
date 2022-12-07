import pandas
import sqlalchemy
from app.etl.DataSoruces.DataSource import DataSource 

class SqliteDS(DataSource):
   
    def __init__(self, _sorucePath: str, _destinationPath: str, _operation: dict, _data) -> None:
        super().__init__(_sorucePath, _destinationPath, _operation, _data)
        DataSource.results = None

    def extract(self) -> pandas.DataFrame:
        DBSoruce = self.SourcePath.split('/')[0]
        tableName = self.SourcePath.split('/')[1]

        sqlite_engine = sqlalchemy.create_engine(f'sqlite:///{DBSoruce}')
        return pandas.read_sql(f'select * from {tableName}', sqlite_engine)

    def load(self) -> None:
        DBDestination = self.DestinationPath.split('/')[0]
        tableName = self.DestinationPath.split('/')[1]

        sqlite_engine = sqlalchemy.create_engine(f'sqlite:///{DBDestination}')
        self.Data.to_sql(tableName, sqlite_engine, if_exists='append', index=False)

class MssqlDS(DataSource):
    
    def __init__(self, _sorucePath: str, _destinationPath: str, _operation: dict, _data) -> None:
        super().__init__(_sorucePath, _destinationPath, _operation, _data)
        DataSource.results = None

    def extract(self) -> pandas.DataFrame:
        connection_string = connection_string.split("/")
        server_name = connection_string[0]
        db_name = connection_string[1]
        table_name = connection_string[2]

        mssql_engine = sqlalchemy.create_engine(f'mssql+pyodbc://{server_name}/{db_name}?trusted_connection=yes&driver=SQL+Server+Native+Client+11.0')
        table = mssql_engine.execute(f"SELECT * FROM {table_name};")
    
        data = pandas.DataFrame(table, columns=table.keys())
        return data

    def load(self) -> None:
        connection_string = connection_string.split("/")
        server_name = connection_string[0]
        db_name = connection_string[1]
        table_name = connection_string[2]

        mssql_engine = sqlalchemy.create_engine(f'mssql+pyodbc://{server_name}/{db_name}?trusted_connection=yes&driver=SQL+Server+Native+Client+11.0')
        self.Data.to_sql(table_name, mssql_engine, if_exists='append', index=False)
