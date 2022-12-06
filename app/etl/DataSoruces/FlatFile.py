import pandas
import regex as re
from app.etl.DataSoruces.DataSource import DataSource 


class CsvDS(DataSource):
    
    def __init__(self, _sorucePath: str, _destinationPath: str, _operation: dict, _data) -> None:
        super().__init__(_sorucePath, _destinationPath, _operation, _data)
        DataSource.results = None
        
    def extract(self) -> pandas.DataFrame:
        return pandas.read_csv(self.SourcePath)

    def load(self) -> None:
        self.Data.to_csv(self.DestinationPath, mode='a')
        DataSource.results = 'Execution Done!'

class HtmlDS(DataSource):
    
    def extract(self) -> pandas.DataFrame:
        return pandas.read_html(self.SourcePath)

    def load(self) -> None:
        self.Data.to_html(self.DestinationPath)

class JsonDF(DataSource):
    
    def extract(self) -> pandas.DataFrame:
        return pandas.read_json(self.SourcePath, orient='records')
    
    def load(self) -> None:
        self.Data.to_json(self.DestinationPath)

class XmlDS(DataSource):
    
    def extract(self) -> pandas.DataFrame:
        return pandas.read_xml(self.SourcePath)

    def load(self) -> None:
        self.Data.to_xml(self.DestinationPath)

class ExcelDS(DataSource):
    
    def extract(self) -> pandas.DataFrame:
        return pandas.read_excel(self.SourcePath)

    def load(self) -> None:
        self.Data.to_excel(self.DestinationPath) 

class ConsolDS(DataSource):

    def load(self) -> None:
        pass


# if __name__ == '__main__':
#     DataOp = {
#         'COLUMNS':  '__all__',
#         'DISTINCT': False,
#         'FILTER':   None,
#         'ORDER':    None,
#         'LIMIT':    None,
#     }
#     csv = CsvDS("csv::c:\\project\\annual-enterprisesv.csv" , "csv::c:\\project\\annual-enterprisesv-baher.csv", DataOp)
#     csv.extract()
#     csv.DataSourceFunctions['Coulmns']
#     csv.load()


# from app.etl.DataSoruces.DataSource.FlatFile import CsvDS
# DataOp = {
#         'COLUMNS':  '__all__',
#         'DISTINCT': False,
#         'FILTER':   None,
#         'ORDER':    None,
#         'LIMIT':    None,
#        }
# csv = CsvDS(csv::c:\\project\\annual-enterprisesv.csv , csv::c:\\project\\annual-enterprisesv-new.csv, DataOp)
# csv.extract()
# csv.DataSourceFunctions['Coulmns']
# csv.load()