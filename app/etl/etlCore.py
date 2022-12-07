from app.etl.DataSoruces.FlatFile import *
from app.etl.DataSoruces.Database import *

class etl:
    ClassType = {
            "csv": lambda x: CsvDS(x),
            "sqlite": lambda x: SqliteDS(x),
            "mssql": lambda x: MssqlDS(x),
            "html": lambda x: HtmlDS(x),
            "json": lambda x: JsonDF(x),
            "xml": lambda x: XmlDS(x),
            "excel": lambda x: ExcelDS(x)
    }
    def __init__(self, _sorucePath:str, _destinationPath:str, _operation:dict) -> None:
        self.SourcePath = _sorucePath.split('::')[1].lower()
        self.SourceType = _sorucePath.split('::')[0].lower()
        self.DestinationPath = _destinationPath.split('::')[1].lower()
        self.DestinationType = _destinationPath.split('::')[0].lower()
        self.Operation = _operation
        self.Data = pandas.DataFrame()

    # Get DataSoruce Object base one Source file type
    def __SoruceType(self) -> DataSource:
        return self.ClassType[self.SourceType](None)

    # Get DataSoruce Object base one Destination file type
    def __DestinationType(self) -> DataSource:
        return self.ClassType[self.DestinationType](self.Data)

    def ExtractData(self):
        self.Data = self.__SoruceType().extract(self.SourcePath)

    def TransformData(self):
        self.Data = self.__DestinationType().transform(self.Operation)

    def LoadData(self):
        self.__DestinationType().load(self.DestinationPath)
