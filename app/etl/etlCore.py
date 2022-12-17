from app.etl.DataSoruces.FlatFile import *
from app.etl.DataSoruces.Database import *
from app.etl.DataSoruces.BinaryFile import BirdDetector

class etl:
    ClassType = {
            "csv": lambda x: CsvDS(x),
            "sqlite": lambda x: SqliteDS(x),
            "mssql": lambda x: MssqlDS(x),
            "html": lambda x: HtmlDS(x),
            "json": lambda x: JsonDF(x),
            "xml": lambda x: XmlDS(x),
            "excel": lambda x: ExcelDS(x),
            "console" : lambda x: ConsolDS(x)
            
    }
    def __init__(self, _sorucePath:str, _destinationPath:str, _operation:dict) -> None:
        self.isThread = False
        self.SourceType = _sorucePath.split('::')[0].lower()
        self.SourcePath = _sorucePath.split('::')[1].lower()
        self.DestinationType = _destinationPath.split('::')[0].lower()
        self.DestinationPath = _destinationPath.split('::')[1].lower() if _destinationPath.split('::')[0].lower() != "console" else None
        self.Operation = _operation
        self.Data = pandas.DataFrame()

    # Get DataSoruce Object base one Source file type
    def __SoruceType(self, _data = None) -> DataSource:
        return self.ClassType[self.SourceType](_data, self.isThread)

    # Get DataSoruce Object base one Destination file type
    def __DestinationType(self, _data = None) -> DataSource:
        return self.ClassType[self.DestinationType](_data, self.isThread)

    def ExtractData(self):
        self.ExtrData = self.__SoruceType().extract(self.SourcePath)

    def TransformData(self):
        self.TransData = self.__DestinationType(self.ExtrData).transform(self.Operation)

    def LoadData(self):
        self.__DestinationType(self.TransData).load(self.DestinationPath)

    def RunCode(self):
        pass

    def StartThread(self):
        self.isThread = True

    def EndThread(self):
        pass