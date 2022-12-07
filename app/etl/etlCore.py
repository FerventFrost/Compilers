from app.etl.DataSoruces.FlatFile import *
from app.etl.DataSoruces.Database import *

class etl:
    ClassType = {
            "csv": lambda x,y,z,m: CsvDS(x, y, z,m),
            "sqlite": lambda x,y,z,m: SqliteDS(x, y,z,m),
            "mssql": lambda x,y,z,m: MssqlDS(x, y,z,m),
            "html": lambda x,y,z,m: HtmlDS(x, y,z,m),
            "json": lambda x,y,z,m: JsonDF(x, y,z,m),
            "xml": lambda x,y,z,m: XmlDS(x, y,z,m),
            "excel": lambda x,y,z,m: ExcelDS(x, y,z,m)
    }
    def __init__(self, _sorucePath:str=None, _destinationPath:str=None, _operation:dict=None) -> None:
        self.SourcePath = _sorucePath.split('::')[1].lower()
        self.SourceType = _sorucePath.split('::')[0].lower()
        self.Destination = _destinationPath.split('::')[1].lower()
        self.DestinationType = _destinationPath.split('::')[0].lower()
        self.Operation = _operation
        self.Data = None

    # Get DataSoruce Object base one Source file type
    def __SoruceType(self) -> DataSource:
        return self.ClassType[self.SourceType](self.SourcePath,self.Destination,self.Operation,None)

    # Get DataSoruce Object base one Destination file type
    def __DestinationType(self) -> DataSource:
        return self.ClassType[self.DestinationType](self.SourcePath,self.Destination,self.Operation,self.Data)

    def ExtractData(self):
        self.Data = self.__SoruceType().extract()

    def TransformData(self):
        self.Data = self.__DestinationType().Transform()

    def LoadData(self):
        self.__DestinationType().load()
