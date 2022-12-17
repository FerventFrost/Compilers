import numpy as np
from queue import Queue
import cv2
from app.BirdDetector.Utility import DetectorUilityClass

class DetectorReader(DetectorUilityClass):

    def __init__(self) -> None:
        super().__init__()
        self.Queue = Queue()

    def setSource(self, source):
        self.Source = source

    def initReader(self):
        self.Queue.put("video")
        self.readVideoStream(cv2.VideoCapture(self.Source))

    def readVideoStream(self, videoStream):
        while(True):
            ret, frame = videoStream.read()
            if not ret:
                self.Queue.put(None)
                break
            self.readFrame(frame)

    def readFrame(self, frame):
        frame = cv2.resize(frame, (1024,1124))
        self.Queue.put(frame)

    def returnQueue(self):
        return self.Queue
        
    def run(self):
        self.initReader()  
      
class DetectorDetect(DetectorUilityClass):

    def __init__(self, _readerQueue:Queue) -> None:
        super().__init__()
        self.ReaderQueue = _readerQueue
        self.WriterQueue = Queue()
        self.readCocoClass(self.classespath)
        self.downloadModel(self.modelURL)
        self.loadModel()
  
    def initDetect(self):
        number = 0
        while True:
            frame = self.ReaderQueue.get()
            if frame is None:
                self.WriterQueue.put(None)
                break
            
            if isinstance(frame, str):
                self.WriterQueue.put(frame)
            else:
                self.WriterQueue.put(self.detectBird(frame))

    def detectBird(self, image):
        imH, imW, _ = image.shape
        detections = self.Model(self.formatTensorInput(image))
        dataProperties = self.detectedObjectData(detections)

        if len(dataProperties[1]) != 0:
            image = self.labelDetectedObject(image, dataProperties, imW, imH)
        return image
        
    def labelDetectedObject(self, image, dataProperties, w, h):
        BirdNumber = 0

        for i in dataProperties[1]:
            Confidence = round(100*dataProperties[3][i])
            Label = self.ClassList[dataProperties[2][i]]
            Color = self.ColorList[dataProperties[2][i]]

            bPostion = self.calcBBox(tuple(dataProperties[0][i].tolist()), w, h)

            if self.isTargetObject('bird', Label):
                self.makeRectAroundObject(image, Color, bPostion)
                self.LabelObject(image, f"{Label}: {Confidence}", Color, bPostion)
                BirdNumber += 1

        self.LabelObject(image, f"Birds Count {BirdNumber}", (200, 10, 191), (50, 50))
        return image

    def returnQueue(self):
        return self.WriterQueue

    def run(self):
        self.initDetect()

class DetectorWriter(DetectorUilityClass):

    def __init__(self, _queue) -> None:
        super().__init__()
        self.Queue = _queue

    def initWriter(self):
        if self.Queue.get() == "frame":
            self.viewFrame()
        else:
            self.viewVideo()
        cv2.destroyAllWindows()

    def viewFrame(self):
        cv2.imshow("Result", self.Queue.get())
        cv2.waitKey(0)

    def viewVideo(self):
        while True:
            frame = self.Queue.get()
            if frame is None:
                self.Queue.put(None)
                break
            if cv2.waitKey(40) & 0xFF == ord('q'):
                break
            cv2.imshow("Result", frame)

    def returnQueue(self):
        return self.Queue

    def run(self):
        self.initWriter()
