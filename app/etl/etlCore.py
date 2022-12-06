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
        self.SourcePath = _sorucePath
        self.Destination = _destinationPath
        self.Operation = _operation
        self.Data = None

    def __SoruceType(self) -> DataSource:
        temp = self.SourcePath.split('::')[0].lower()
        return self.ClassType[temp](self.SourcePath,self.Destination,self.Operation,None)

    def __DestinationType(self) -> DataSource:
        temp = self.Destination.split("::")[0].lower()
        return self.ClassType[temp](self.SourcePath,self.Destination,self.Operation,self.Data)

    def ExtractData(self):
        self.Data = self.__SoruceType().extract()

    # def TransformData(self):
    #     self.Data = 

    def LoadData(self):
        self.__DestinationType().load()
