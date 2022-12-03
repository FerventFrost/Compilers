import pandas

class compilerAggregationFunction:
    def __init__(self, _data:pandas.DataFramem, _filters:dict) -> None:
        self.Data = _data
        self.Left = _filters['left']
        self.right = _filters['right']

    def Or(self):
        pass

    def And(self):
        pass

    def Like(self):
        pass

    def GreaterThan(self):
        pass

    def GreaterOrEqual(self):
        pass

    def LessThan(self):
        pass

    def LessOrEqual(self):
        pass

    def Equal(self):
        pass

    def NotEqual(self):
        pass

class DataSource:

    def __init__(self, _filePath:str, _data:pandas.DataFrame, _operation:dict) -> None:
        self.dfName = None
        self.FilePath = _filePath
        self.Data = _data
        self.Operation = _operation
        # self.Agg = compilerAggregationFunction()

    def extract(self, type = None) -> None:
        raise NotImplementedError

    def load(self, type = None) -> None:
        raise NotImplementedError

    def getColumns(self) -> pandas.DataFrame:
        pass

    def getOrder(self) -> pandas.DataFrame:
        pass

    def getLimit(self) -> pandas.DataFrame:
        pass

class CsvDS(DataSource):
    pass

class SqliteDS(DataSource):
    pass

class MssqlDS(DataSource):
    pass

class HtmlDS(DataSource):
    pass

class JsonDF(DataSource):
    pass

class XmlDS(DataSource):
    pass

class ExcelDS(DataSource):
    pass

class ConsolDS(DataSource):
    pass