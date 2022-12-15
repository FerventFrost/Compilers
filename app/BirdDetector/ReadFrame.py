import numpy as np
import cv2, tensorflow as tf
from app.BirdDetector.Utility import DetectorUilityClass

class DetectorReader(DetectorUilityClass):

    def __init__(self) -> None:
        super.__init__()

    def getPath(self, source):
        Path = source.split("::")[1]
        if source.split("::")[0] == 'mp4':
            self.readVideoStream(cv2.VideoCapture(Path))
        else:
            self.readFrame(cv2.imread(Path))

    def readVideoStream(self, videoStream):
        while(True):
            ret, frame = videoStream.read()
            if not ret or cv2.waitKey(1) & 0xFF == ord('q'):
                break

            self.readFrame(frame, True)
        cv2.destroyAllWindows()

    def readFrame(self, frame, isVideo = False):
        frame = cv2.resize(frame, (1024,1124))
        detectedImage = self.detectBird(frame)
        cv2.imshow("Result", detectedImage)
        if not isVideo:
            cv2.waitKey(0)
            cv2.destroyAllWindows()


class DetectorDetect(DetectorUilityClass):

    def __init__(self) -> None:
        super().__init__()

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

    def __init__(self) -> None:
        super().__init__()
