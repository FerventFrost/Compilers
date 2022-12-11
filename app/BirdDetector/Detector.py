import os, time
import numpy as np
import cv2, tensorflow as tf
from tensorflow.python.keras.utils.data_utils import get_file


class Detector:
    
    def __init__(self) -> None:
        self.CacheDir, self.ClassList, self.ColorList, self.ModelName, self.Model = None,None,None,None,None
        np.random.seed(123)
        classespath = './app/BirdDetector/coco.names'
        modelURL = 'http://download.tensorflow.org/models/object_detection/tf2/20200711/ssd_mobilenet_v2_320x320_coco17_tpu-8.tar.gz'
        self.readClass(classespath)
        self.downloadModel(modelURL)
        self.loadModel()
   
    #Make a Random Color for every class in the file
    def __classColorList(self, __classList:list[str]) -> list[int]:
        return np.random.uniform(low=0, high=255, size=(len(__classList), 3))

    #Make File to Save Downloaded PreTrained Models
    def __preTrainedModelDir(self) -> str:
        os.makedirs('Pretrained_Models', exist_ok=True)
        return 'Pretrained_Models'

    #Convert Frame to TensorFlow array
    def __formatTensorInput(self, __frame) -> tf:
        input = cv2.cvtColor(__frame.copy(), cv2.COLOR_BGR2RGB)
        input = tf.convert_to_tensor(input, dtype=tf.uint8)
        return input[tf.newaxis,...]

    #Return detected image data like postion, class, etc
    def __detectionData(self, __detection) -> tuple:
        bboxs = __detection['detection_boxes'][0].numpy()
        classIndexes = __detection['detection_classes'][0].numpy().astype(np.int32)
        classScores = __detection['detection_scores'][0].numpy()
        bboxIdx = tf.image.non_max_suppression(bboxs, classScores, max_output_size = 50, 
        iou_threshold=0.5, score_threshold=0.5)
        return (bboxIdx, classIndexes, classScores, bboxs)
        
    #Calculate the box around the detected postion
    def __calcBBox(self, __bBox:tuple, __w, __h) -> tuple[int,int,int,int]:
        ymin, xmin, ymax, xmax = __bBox       
        xmin, xmax, ymin, ymax = (xmin*__w, xmax*__w, ymin*__h, ymax*__h)
        return (int(xmin), int(xmax), int(ymin), int(ymax))

    def __LabeledObject(self, __frame, __targetedObject:str, __labeledObject:str, __displayText:str, __color, __recPostion:tuple[int,int,int,int]):
        if __labeledObject == __targetedObject:
            cv2.rectangle(__frame, (__recPostion[0], __recPostion[2]), (__recPostion[1], __recPostion[3]), color=__color, thickness=1)
            cv2.putText(__frame, __displayText, (__recPostion[0], __recPostion[2] - 10), cv2.FONT_HERSHEY_PLAIN, 1, __color, 2)
            return True
        return False

    def readClass(self, classFile) -> None:
        #read File
        with open(classFile, 'r') as f:
            self.ClassList = f.read().splitlines()
        self.ColorList = self.__classColorList(self.ClassList)

    def downloadModel(self, modelURL:str) -> None:
        FileName = os.path.basename(modelURL)
        self.ModelName = FileName[:FileName.index('.')]
        self.CacheDir = self.__preTrainedModelDir()

        get_file(fname=FileName, origin=modelURL, cache_dir= self.CacheDir, cache_subdir="CheckPoints", extract=True)

    def loadModel(self) -> None:
        savedModelPath = os.path.join(self.CacheDir, "CheckPoints", self.ModelName, "saved_model")
        tf.keras.backend.clear_session()
        self.Model = tf.saved_model.load(savedModelPath)

    def detectBird(self, image):
        imH, imW, imD = image.shape
        BirdNumber = 0
        detections = self.Model(self.__formatTensorInput(image))
        dataProperties = self.__detectionData(detections)

        if len(dataProperties[0]) != 0:
            for i in dataProperties[0]:
                classConfidence = round(100*dataProperties[2][i])
                classLabelText = self.ClassList[dataProperties[1][i]]
                classColor = self.ColorList[dataProperties[1][i]]
                displayText = f"{classLabelText}: {classConfidence}"

                bPostion = self.__calcBBox(tuple(dataProperties[3][i].tolist()),imW, imH)

                if self.__LabeledObject(image, 'bird', classLabelText, displayText, classColor, bPostion):
                    BirdNumber += 1
            
            cv2.putText(image, f"Birds Count {BirdNumber}", (50,50),cv2.FONT_HERSHEY_PLAIN, 1, (200, 10, 191), 2)
            return image

    def readFrame(self, frame, isVideo = False):
        frame = cv2.resize(frame, (1024,1124))
        detectedImage = self.detectBird(frame)
        cv2.imshow("Result", detectedImage)
        if not isVideo:
            cv2.waitKey(0)
            cv2.destroyAllWindows()
        
    def readVideoStream(self, videoStream):
        while(True):
            ret, frame = videoStream.read()
            if not ret or cv2.waitKey(1) & 0xFF == ord('q'):
                break

            self.readFrame(frame, True)
        cv2.destroyAllWindows()

    def readFile(self, source):
        Path = source.split("::")[1]
        if source.split("::")[0] == 'mp4':
            self.readVideoStream(cv2.VideoCapture(Path))
        else:
            self.readFrame(cv2.imread(Path))

if __name__ == '__main__':
    videoPath = "mp4::./Test/BirdVideo.mp4"
    classespath = './app/BirdDetector/coco.names'
    modelURL = 'http://download.tensorflow.org/models/object_detection/tf2/20200711/ssd_mobilenet_v2_320x320_coco17_tpu-8.tar.gz'
    d = Detector()
    d.readClass(classespath)
    d.downloadModel(modelURL)
    d.loadModel()
    d.readFile(videoPath)