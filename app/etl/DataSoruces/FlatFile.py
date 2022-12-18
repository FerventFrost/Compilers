import pandas
from queue import Queue
from app.etl.DataSoruces.DataSource import DataSource 


class CsvDS(DataSource):
    
    def __init__(self, _data: Queue[pandas.DataFrame] = None, _isThread=False) -> None:
        super().__init__(_data, _isThread)
        DataSource.results = None
        
    def extract(self, _sorucePath:str) -> pandas.DataFrame:
        self.TargetMethod = "extract"
        q = Queue()
        if not self.isThread:
            q.put(pandas.read_csv(_sorucePath))
        return q

    def load(self, _destinationPath:str) -> None:
        self.TargetMethod = "load"
        if not self.isThread:
            self.QueueData().to_csv(_destinationPath, mode='a',index=False)
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
        self.QueueData().to_html(_destinationPath)
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
        self.QueueData().to_json(_destinationPath)
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
        self.QueueData().to_xml(_destinationPath)
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
        self.QueueData().to_excel(_destinationPath) 
        DataSource.results = 'Execution Done!'

class ConsolDS(DataSource):

    def __init__(self, _data: Queue[pandas.DataFrame] = None, _isThread=False) -> None:
        super().__init__(_data, _isThread)
        DataSource.results = None

    def load(self, _destinationPath:str) -> None:
        DataSource.results = self.QueueData()