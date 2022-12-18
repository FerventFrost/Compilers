from app.FrameProcessor.Loader.loader import Loader
from app.FrameProcessor.Extract.extract import Extract
from queue import Queue
import pandas

class FolderDS:

    def __init__(self, data = None, _isThread = False) -> None:
        self.Data = data
        self.Dict = []

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
                new_operation = ["time", "head", "tail", "wing" ,"leg", "motion"]
                list.append(obj.extract_all(frame))
        self.noMotion(list)
        df =  self.to_df(_operation, new_operation)
        q.put(df)
        return q

    def to_df(self, _operation, new):
        if new != None:
            df = pandas.DataFrame(self.Dict, columns=new)
        else:
            _operation['COLUMNS'].append("motion")
            df = pandas.DataFrame(self.Dict, columns=_operation['COLUMNS'])
        return df

    def noMotion(self,list):
        prevFrame = list[0]
        
        for i in range(1,len(list),1):
            motion = self.isMotion(list[i], prevFrame)
            if motion:
                prevFrame = list[i]
                temp = list[i].copy()
                temp.append(motion)
                self.Dict.append(temp)
            else:
                temp = prevFrame.copy()
                temp.append(motion)
                self.Dict.append(temp)

        # self.Dict = [ element.split("&") for element in self.Dict]

    def isMotion(self,frame, prev):
        frame = frame[1:5]
        prev = prev[1:5]
        if frame != prev:
            return True
        return False
    
    def returnQueue(self):
        return self.Data.get()
