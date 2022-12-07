import pandas
import sqlalchemy
import regex as re

class compilerAggregationFunction:
    def __init__(self, _data:pandas.DataFrame, _filters:dict) -> None:
        
        if _filters:
            self.Data = _data
            self.left = _filters['left']
            self.right = _filters['right']

            self.AggFunctions = {
            'or' : lambda : self.Or(),
            'and' : lambda : self.And(),
            'like' : lambda : self.Like(),
            '>' : lambda : self.GreaterThan(),
            '>=' : lambda : self.GreaterOrEqual(),
            '<' : lambda : self.LessThan(),
            '<=' : lambda : self.LessOrEqual(),
            '==' : lambda : self.Equal(),
            '!=' : lambda : self.NotEqual(),
        }

    def Or(self):
        self.Data = pandas.concat([self.left, self.right])
        return self.Data[~ self.Data.index.duplicated(keep='first')]

    def And(self):
        self.Data = pandas.merge([self.left, self.right])
        return self.Data[~ self.Data.index.duplicated(keep='first')]

    def Like(self):
        return self.Data[[True if re.match(self.right, str(x)) else False for x in self.Data[self.left]]]

    def GreaterThan(self):
        return self.Data[self.Data[self.left] > self.right]

    def GreaterOrEqual(self):
        return self.Data[self.Data[self.left] >= self.right]

    def LessThan(self):
        return self.Data[self.Data[self.left] < self.right]

    def LessOrEqual(self):
        return self.Data[self.Data[self.left] <= self.right]

    def Equal(self):
        return self.Data[self.Data[self.left] == self.right]

    def NotEqual(self):
        return self.Data[self.Data[self.left] != self.right]

class DataSource:
    results = None
    def __init__(self, _sorucePath:str, _destinationPath:str, _operation:dict, _data) -> None:
        self.dfName = None
        self.SourcePath = _sorucePath
        self.DestinationPath = _destinationPath
        self.Data = _data
        self.Operation = _operation


    def extract(self) -> None:
        raise NotImplementedError

    def load(self) -> None:
        raise NotImplementedError

    def Transform(self) -> pandas.DataFrame:
        Data = None
        if self.Operation['FILTER']:
            Data = self.getFilter()
        if self.Operation['COLUMNS']  != '__all__':
            Data = self.getColumns()
        if self.Operation['DISTINCT']:
            Data = self.getDistinct()
        if self.Operation['ORDER']:
            Data = self.getOrder()
        if self.Operation['LIMIT']:
            Data= self.getLimit()
        return Data

    # return Filtered Data
    def getFilter(self):
        Agg = compilerAggregationFunction(self.Data, self.Operation['FILTER'])
        filter = self.Operation['FILTER']['type']
        return Agg.AggFunctions[filter]

    # return specific columns
    def getColumns(self) -> pandas.DataFrame:
        return self.Data.filter(items= self.Operation['COLUMNS'])

    def getDistinct(self) -> pandas.DataFrame:
        return self.Data.filter(items= self.Operation['COLUMNS'])

    # sort data
    def getOrder(self) -> pandas.DataFrame:
        columnn = self.Operation['ORDER'][0]
        return self.Data.sort_values(columnn, ascending= self.Operation['ORDER'][1] == 'ASC')

    # return data in a certain range
    def getLimit(self) -> pandas.DataFrame:
        return self.Data[:self.Operation['LIMIT']]

