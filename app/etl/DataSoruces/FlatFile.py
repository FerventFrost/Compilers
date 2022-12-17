import pandas
from queue import Queue
from app.etl.DataSoruces.DataSource import DataSource 


class CsvDS(DataSource):
    
    def __init__(self, _data: Queue[pandas.DataFrame] = None, _isThread=False) -> None:
        super().__init__(_data, _isThread)
        DataSource.results = None
        
    def extract(self, _sorucePath:str) -> pandas.DataFrame:
        q = Queue()
        q.put(pandas.read_csv(_sorucePath))
        return q

    def load(self, _destinationPath:str) -> None:
        self.Data.to_csv(_destinationPath, mode='a')
        DataSource.results = 'Execution Done!'

class HtmlDS(DataSource):
    
    def __init__(self, _data: Queue[pandas.DataFrame] = None, _isThread=False) -> None:
        super().__init__(_data, _isThread)
        DataSource.results = None

    def extract(self, _sorucePath:str) -> pandas.DataFrame:
        q = Queue()
        q.put(pandas.read_html(_sorucePath))
        return q

    def load(self, _destinationPath:str) -> None:
        self.Data.to_html(_destinationPath)
        DataSource.results = 'Execution Done!'

class JsonDF(DataSource):
    
    def __init__(self, _data: Queue[pandas.DataFrame] = None, _isThread=False) -> None:
        super().__init__(_data, _isThread)
        DataSource.results = None

    def extract(self, _sorucePath:str) -> pandas.DataFrame:
        q = Queue()
        q.put(pandas.read_json(_sorucePath, orient='records'))
        return q
    
    def load(self, _destinationPath:str) -> None:
        self.Data.to_json(_destinationPath)
        DataSource.results = 'Execution Done!'

class XmlDS(DataSource):
    
    def __init__(self, _data: Queue[pandas.DataFrame] = None, _isThread=False) -> None:
        super().__init__(_data, _isThread)
        DataSource.results = None

    def extract(self, _sorucePath:str) -> pandas.DataFrame:
        q = Queue()
        q.put(pandas.read_xml(_sorucePath))
        return q

    def load(self, _destinationPath:str) -> None:
        self.Data.to_xml(_destinationPath)
        DataSource.results = 'Execution Done!'

class ExcelDS(DataSource):
    
    def __init__(self, _data: Queue[pandas.DataFrame] = None, _isThread=False) -> None:
        super().__init__(_data, _isThread)
        DataSource.results = None

    def extract(self, _sorucePath:str) -> pandas.DataFrame:
        q = Queue()
        q.put(pandas.read_excel(_sorucePath))
        return q

    def load(self, _destinationPath:str) -> None:
        self.Data.to_excel(_destinationPath) 
        DataSource.results = 'Execution Done!'

class ConsolDS(DataSource):

    def __init__(self, _data: Queue[pandas.DataFrame] = None, _isThread=False) -> None:
        super().__init__(_data, _isThread)
        DataSource.results = None

    def load(self, _destinationPath:str) -> None:
        DataSource.results = self.Data