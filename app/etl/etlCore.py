from app.etl.DataSoruces.FlatFile import *
from app.etl.DataSoruces.Database import *

class etl:
    def __init__(self, _sorucePath:str, _destinationPath:str, _operation:dict) -> None:
        self.SourcePath = _sorucePath
        self.Destination = _destinationPath
        self.Operation = _operation

        self.ClassType = self.ChooseClass()

    def ChooseClass(self) -> object:
        ClassType = {
            "csv": lambda x,y,z: CsvDS(x, y, z),
            "sqlite": lambda x,y,z: SqliteDS(x, y,z),
            "mssql": lambda x,y,z: MssqlDS(x, y,z),
            "html": lambda x,y,z: HtmlDS(x, y,z),
            "json": lambda x,y,z: JsonDF(x, y,z),
            "xml": lambda x,y,z: XmlDS(x, y,z),
            "excel": lambda x,y,z: ExcelDS(x, y,z)
        }
        temp = self.SourcePath.split('::')[0].lower()
        return ClassType[temp](self.SourcePath,self.Destination,self.Operation)