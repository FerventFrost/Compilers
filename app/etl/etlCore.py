from app.etl.DataSoruces.FlatFile import *
from app.etl.DataSoruces.Database import *
from app.etl.DataSoruces.BinaryFile import BirdDetector

class etl:
    ClassType = {
            "csv": lambda x, y: CsvDS(x,y),
            "sqlite": lambda x, y: SqliteDS(x,y),
            "mssql": lambda x, y: MssqlDS(x,y),
            "html": lambda x, y: HtmlDS(x,y),
            "json": lambda x, y: JsonDF(x,y),
            "xml": lambda x, y: XmlDS(x,y),
            "excel": lambda x, y: ExcelDS(x,y),
            "console" : lambda x, y: ConsolDS(x,y)
            
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
        self.ExtrClass = self.__SoruceType()
        self.ExtrData = self.ExtrClass.extract(self.SourcePath)

    def TransformData(self):
        self.TransClass = self.__DestinationType(self.ExtrData)
        self.TransData = self.TransClass.transform(self.Operation)

    def LoadData(self):
        self.LoadClass = self.__DestinationType(self.TransData)
        self.LoadClass.load(self.DestinationPath)

    def SetupThread(self):
        self.isThread = True
    
    def StartThread(self):
        pass
    
    def EndThread(self):
        pass


