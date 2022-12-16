import numpy as np
from queue import Queue
import cv2, tensorflow as tf
from app.BirdDetector.Utility import DetectorUilityClass

class DetectorReader(DetectorUilityClass):

    def __init__(self, _queue:Queue) -> None:
        super.__init__()
        self.Queue = _queue
        

    def initReader(self, source):
        Path = source.split("::")[1]
        if source.split("::")[0] == 'video':
            self.Queue.put("video")
            self.readVideoStream(cv2.VideoCapture(Path))
        else:
            self.Queue.put("frame")
            self.readFrame(cv2.imread(Path))
            self.readFrame(None)

    def readVideoStream(self, videoStream):
        while(True):
            ret, frame = videoStream.read()
            if not ret or cv2.waitKey(1) & 0xFF == ord('q'):
                self.readFrame(None)
                break
            self.readFrame(frame)

    def readFrame(self, frame):
        if frame is None:
            self.Queue.put(None)

        frame = cv2.resize(frame, (1024,1124))
        self.Queue.put(frame)

class DetectorDetect(DetectorUilityClass):

    def __init__(self, _readerQueue:Queue, _writerQueue:Queue) -> None:
        super().__init__()
        self.ReaderQueue = _readerQueue
        self.WriterQueue = _writerQueue

    def initDetector(self):
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
        detections = self.Model(self.__formatTensorInput(image))
        dataProperties = self.__detectedObjectData(detections)

        if len(dataProperties[1]) != 0:
            image = self.labelDetectedObject(image, dataProperties, imW, imH)

        return image
        
    def labelDetectedObject(self, image, dataProperties, w, h):
        BirdNumber = 0

        for i in dataProperties[1]:
            Confidence = round(100*dataProperties[3][i])
            Label = self.ClassList[dataProperties[2][i]]
            Color = self.ColorList[dataProperties[2][i]]

            bPostion = self.__calcBBox(tuple(dataProperties[3][i].tolist()), w, h)

            if self.__isTargetObject('bird', Label):
                self.__makeRectAroundObject(image, Color, bPostion)
                self.__LabelObject(image, f"{Label}: {Confidence}", Color, bPostion[:2])
                BirdNumber += 1

        self.__LabelObject(image, f"Birds Count {BirdNumber}", (200, 10, 191), (50,50))
        return image
        

class DetectorWriter(DetectorUilityClass):

    def __init__(self, _queue:Queue) -> None:
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
                break
            cv2.imshow("Result", frame)
