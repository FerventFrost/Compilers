from app.BirdDetector.ReadFrame import *

class BirdDetector:
    
    def __init__(self, _data = None, _isThread = False) -> None:
        self.Data = _data.returnQueue()
        self.isThread = _isThread
        # if _isThread and _data is not None:
        #     self.Data = _data.returnQueue()

    def extract(self, _sorucePath):
        reader = DetectorReader()
        reader.initReader(_sorucePath)
        # if not self.isThread:
        #     reader.run()
        #     reader = reader.returnQueue()
        return reader

    def transform(self, _):
        transform = DetectorDetect(self.Data)
        # if not self.isThread:
        #     transform.run()
        #     transform = transform.returnQueue()
        return transform
        

    def load(self, _):
        load = DetectorWriter()
        # if not self.isThread:
        #     load.run()
        #     load = load.returnQueue()
        return load