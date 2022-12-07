import pandas
from app.etl.DataSoruces.DataSource import DataSource 


class CsvDS(DataSource):
    
    def __init__(self, _data:pandas.DataFrame = None) -> None:
        super().__init__(_data)
        DataSource.results = None
        
    def extract(self, _sorucePath:str) -> pandas.DataFrame:
        return pandas.read_csv(_sorucePath)

    def load(self, _destinationPath:str) -> None:
        self.Data.to_csv(_destinationPath, mode='a')
        DataSource.results = 'Execution Done!'

class HtmlDS(DataSource):
    
    def __init__(self, _data:pandas.DataFrame = None) -> None:
        super().__init__(_data)
        DataSource.results = None

    def extract(self, _sorucePath:str) -> pandas.DataFrame:
        return pandas.read_html(_sorucePath)

    def load(self, _destinationPath:str) -> None:
        self.Data.to_html(_destinationPath)
        DataSource.results = 'Execution Done!'

class JsonDF(DataSource):
    
    def __init__(self, _data:pandas.DataFrame = None) -> None:
        super().__init__(_data)
        DataSource.results = None

    def extract(self, _sorucePath:str) -> pandas.DataFrame:
        return pandas.read_json(_sorucePath, orient='records')
    
    def load(self, _destinationPath:str) -> None:
        self.Data.to_json(_destinationPath)
        DataSource.results = 'Execution Done!'

class XmlDS(DataSource):
    
    def __init__(self, _data:pandas.DataFrame = None) -> None:
        super().__init__(_data)
        DataSource.results = None

    def extract(self, _sorucePath:str) -> pandas.DataFrame:
        return pandas.read_xml(_sorucePath)

    def load(self, _destinationPath:str) -> None:
        self.Data.to_xml(_destinationPath)
        DataSource.results = 'Execution Done!'

class ExcelDS(DataSource):
    
    def __init__(self, _data:pandas.DataFrame = None) -> None:
        super().__init__(_data)
        DataSource.results = None

    def extract(self, _sorucePath:str) -> pandas.DataFrame:
        return pandas.read_excel(_sorucePath)

    def load(self, _destinationPath:str) -> None:
        self.Data.to_excel(_destinationPath) 
        DataSource.results = 'Execution Done!'

class ConsolDS(DataSource):

    def __init__(self, _data = None) -> None:
        super().__init__(_data)
        DataSource.results = None

    def load(self, _destinationPath:str) -> None:
        pass
