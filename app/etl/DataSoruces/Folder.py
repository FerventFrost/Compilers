from app.FrameProcessor.Loader.loader import Loader
from app.FrameProcessor.Extract.extract import Extract
from queue import Queue
import pandas

class FolderDS:

    def __init__(self, data = None, _isThread = False) -> None:
        self.Data = data
        self.Dict = set()

    def extract(self, _source):
        obj = Loader()
        q = Queue()
        q.put(obj.load(_source))
        return q

    def transform(self, _operation):
        list = []
        obj = Extract()
        q = Queue()
        data = self.returnQueue()
        new_operation = None
        for frame in data:
            if _operation['COLUMNS']  != '__all__':  
                list.append(obj.extract(_operation['COLUMNS'] ,frame))
            else:
                new_operation = ["time", "head", "tail", "wing" ,"leg"]
                list.append(obj.extract_all(frame))
        self.noMotion(list)
        df =  self.to_df(_operation, new_operation)
        q.put(df)
        return q

    def to_df(self, _operation, new):
        if new != None:
            df = pandas.DataFrame(self.Dict, columns=new)
        else:
            df = pandas.DataFrame(self.Dict, columns=_operation['COLUMNS'])
        df.sort_values(by=["time"], inplace=True)
        return df

    def noMotion(self,list):
        prevFrame = list[0]
        for i in range(1,len(list),1):
            if self.isMotion(list[i], prevFrame):
                # self.Dict["No Motion"].append(prevFrame)
                self.Dict.add("&".join(prevFrame))
                self.Dict.add("&".join(list[i]))
                prevFrame = list[i]
        
        self.Dict = [ element.split("&") for element in self.Dict]

    def isMotion(self,frame, prev):
        frame = frame[1:]
        prev = prev[1:]
        if frame != prev:
            return True
        return False
    
    def returnQueue(self):
        return self.Data.get()
