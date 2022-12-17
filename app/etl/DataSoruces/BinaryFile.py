from app.BirdDetector.ReadFrame import *

class BirdDetector:
    
    def __init__(self, _data = None, _isThread = False) -> None:
        self.Data = _data
        self.isThread = _isThread

    def extract(self, _sorucePath) -> Queue:
        self.Class = DetectorReader()
        self.Class.setSource(_sorucePath)
        if not self.isThread:
            self.Class.initReader()
        return self.Class.returnQueue()

    def transform(self, _) -> Queue:
        self.Class = DetectorDetect(self.Data)
        if not self.isThread:
            self.Class.initDetect()
        return self.Class.returnQueue()
        

    def load(self, _) -> None:
        self.Class = DetectorWriter(self.Data)
        if not self.isThread:
            self.Class.initWriter()

    def start(self):
        self.Class.start()

    def join(self):
        self.Class.join()
        