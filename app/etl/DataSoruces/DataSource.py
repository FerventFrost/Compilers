import pandas
import sqlalchemy
import regex as re

class compilerAggregationFunction:
    def __init__(self, _data:pandas.DataFrame, _filters:dict) -> None:
        
        if _filters:
            self.Data = _data
            self.left = _filters['left']
            self.right = _filters['right']

            self.AggFunctions = {
            'or' : lambda : self.Or(),
            'and' : lambda : self.And(),
            'like' : lambda : self.Like(),
            '>' : lambda : self.GreaterThan(),
            '>=' : lambda : self.GreaterOrEqual(),
            '<' : lambda : self.LessThan(),
            '<=' : lambda : self.LessOrEqual(),
            '==' : lambda : self.Equal(),
            '!=' : lambda : self.NotEqual(),
        }

    def Or(self):
        self.Data = pandas.concat([self.left, self.right])
        return self.Data[~ self.Data.index.duplicated(keep='first')]

    def And(self):
        self.Data = pandas.merge([self.left, self.right])
        return self.Data[~ self.Data.index.duplicated(keep='first')]

    def Like(self):
        return self.Data[[True if re.match(self.right, str(x)) else False for x in self.Data[self.left]]]

    def GreaterThan(self):
        return self.Data[self.Data[self.left] > self.right]

    def GreaterOrEqual(self):
        return self.Data[self.Data[self.left] >= self.right]

    def LessThan(self):
        return self.Data[self.Data[self.left] < self.right]

    def LessOrEqual(self):
        return self.Data[self.Data[self.left] <= self.right]

    def Equal(self):
        return self.Data[self.Data[self.left] == self.right]

    def NotEqual(self):
        return self.Data[self.Data[self.left] != self.right]

class DataSource:
    results = None
    def __init__(self, _sorucePath:str, _destinationPath:str, _operation:dict) -> None:
        self.dfName = None
        self.SourcePath = _sorucePath.split("::")[1]
        self.DestinationPath = _destinationPath.split("::")[1]
        self.Data = ""
        self.Operation = _operation
        self.Agg = compilerAggregationFunction(self.Data, self.Operation['FILTER'])

        self.DataSourceFunctions = {
            'Filter' : lambda : self.getFilter(),
            'Coulmns' : lambda : self.getColumns(),
            'Order' : lambda : self.getOrder(),
            'Limit' : lambda : self.getLimit(),
            'Distinct' : lambda : self.getDistinct(),
        }

    def extract(self) -> None:
        raise NotImplementedError

    def load(self) -> None:
        raise NotImplementedError

    # return Filtered Data
    def getFilter(self):
        filter = self.Operation['FILTER']['type']
        return self.Agg.AggFunctions[filter]

    # return specific columns
    def getColumns(self) -> pandas.DataFrame:
        return self.Data.filter(items= self.Operation['COLUMNS'])

    def getDistinct(self) -> pandas.DataFrame:
        return self.Data.filter(items= self.Operation['COLUMNS'])

    # sort data
    def getOrder(self) -> pandas.DataFrame:
        columnn = self.Operation['ORDER'][0]
        return self.Data.sort_values(columnn, ascending= self.Operation['ORDER'][1] == 'ASC')

    # return data in a certain range
    def getLimit(self) -> pandas.DataFrame:
        return self.Data[:self.Operation['LIMIT']]

# class CsvDS(DataSource):
    
    def extract(self, type=None) -> pandas.DataFrame:
        return pandas.read_csv(self.SourcePath)

    def load(self, type=None) -> None:
        self.Data.to_csv(self.DestinationPath, mode='a')

# class SqliteDS(DataSource):
   
#     def extract(self, type=None) -> pandas.DataFrame:
#         DBSoruce = self.SourcePath.split('/')[0]
#         tableName = self.SourcePath.spli('/')[1]

#         sqlite_engine = sqlalchemy.create_engine(f'sqlite:///{DBSoruce}')
#         return pandas.read_sql(f'select * from {tableName}', sqlite_engine)

#     def load(self, type=None) -> None:
#         DBDestination = self.DestinationPath.split('/')[0]
#         tableName = self.DestinationPath.split('/')[1]

#         sqlite_engine = sqlalchemy.create_engine(f'sqlite:///{DBDestination}')
#         self.Data.to_sql(tableName, sqlite_engine, if_exists='append', index=False)

# class MssqlDS(DataSource):
    
#     def extract(self, type=None) -> pandas.DataFrame:
#         connection_string = connection_string.split("/")
#         server_name = connection_string[0]
#         db_name = connection_string[1]
#         table_name = connection_string[2]

#         mssql_engine = sqlalchemy.create_engine(f'mssql+pyodbc://{server_name}/{db_name}?trusted_connection=yes&driver=SQL+Server+Native+Client+11.0')
#         table = mssql_engine.execute(f"SELECT * FROM {table_name};")
    
#         data = pandas.DataFrame(table, columns=table.keys())
#         return data

#     def load(self, type=None) -> None:
#         connection_string = connection_string.split("/")
#         server_name = connection_string[0]
#         db_name = connection_string[1]
#         table_name = connection_string[2]

#         mssql_engine = sqlalchemy.create_engine(f'mssql+pyodbc://{server_name}/{db_name}?trusted_connection=yes&driver=SQL+Server+Native+Client+11.0')
#         self.Data.to_sql(table_name, mssql_engine, if_exists='append', index=False)

# class HtmlDS(DataSource):
    
#     def extract(self, type=None) -> pandas.DataFrame:
#         return pandas.read_html(self.SourcePath)

#     def load(self, type=None) -> None:
#         self.Data.to_html(self.DestinationPath)

# class JsonDF(DataSource):
    
#     def extract(self, type=None) -> pandas.DataFrame:
#         return pandas.read_json(self.SourcePath, orient='records')
    
#     def load(self, type=None) -> None:
#         self.Data.to_json(self.DestinationPath)

# class XmlDS(DataSource):
    
#     def extract(self, type=None) -> pandas.DataFrame:
#         return pandas.read_xml(self.SourcePath)

#     def load(self, type=None) -> None:
#         self.Data.to_xml(self.DestinationPath)

# class ExcelDS(DataSource):
    
#     def extract(self, type=None) -> pandas.DataFrame:
#         return pandas.read_excel(self.SourcePath)

#     def load(self, type=None) -> None:
#         self.Data.to_excel(self.DestinationPath) 

# class ConsolDS(DataSource):

#     def load(self, type=None) -> None:
#         pass
