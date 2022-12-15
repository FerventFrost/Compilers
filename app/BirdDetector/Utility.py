import os
import numpy as np
import cv2, tensorflow as tf
from tensorflow.python.keras.utils.data_utils import get_file

class DetectorUilityClass:

    def __init__(self) -> None:
        np.random.seed(123)
        classespath = './app/BirdDetector/coco.names'
        modelURL = 'http://download.tensorflow.org/models/object_detection/tf2/20200711/ssd_mobilenet_v2_320x320_coco17_tpu-8.tar.gz'

    def readCocoClass(self, classFile:str) -> list[int]:
        with open(classFile, 'r') as f:
            self.ClassList = f.read().splitlines()
        self.ColorList = self.__classColorList(self.ClassList)

    def downloadModel(self, modelURL:str) -> None:
        FileName = os.path.basename(modelURL)
        self.ModelName = FileName[:FileName.index('.')]
        self.CacheDir = self.__preTrainedModelDir()

        get_file(fname=FileName, origin=modelURL, cache_dir= self.CacheDir, cache_subdir="CheckPoints", extract=True)

    def loadModel(self) -> tf:
        savedModelPath = os.path.join(self.CacheDir, "CheckPoints", self.ModelName, "saved_model")
        tf.keras.backend.clear_session()
        self.Model = tf.saved_model.load(savedModelPath)

    #Make a Random Color for every class in the coco.name file
    def __makeColorList(self, __classList:list[str]) -> list[int]:
        return np.random.uniform(low=0, high=255, size=(len(__classList), 3))

    #Make File to Save Downloaded PreTrained Models
    def __makePreTrainedModelDir(self) -> str:
        os.makedirs('Pretrained_Models', exist_ok=True)
        return 'Pretrained_Models'

    #Convert cv2 object to TensorFlow array
    def __formatTensorInput(self, __frame) -> tf:
        input = cv2.cvtColor(__frame.copy(), cv2.COLOR_BGR2RGB)
        input = tf.convert_to_tensor(input, dtype=tf.uint8)
        return input[tf.newaxis,...]

    #Return detected image data like postion, class, etc
    def __detectedObjectData(self, __detection) -> tuple:
        bboxs = __detection['detection_boxes'][0].numpy()
        bboxIdx = tf.image.non_max_suppression(bboxs, classScores, max_output_size = 50, 
        iou_threshold=0.5, score_threshold=0.5)

        classIndexes = __detection['detection_classes'][0].numpy().astype(np.int32)
        classScores = __detection['detection_scores'][0].numpy()
        
        return (bboxs, bboxIdx, classIndexes, classScores)
        
    def __calcBBox(self, __bBox:tuple, __w, __h) -> tuple[int,int,int,int]:
        ymin, xmin, ymax, xmax = __bBox       
        xmin, xmax, ymin, ymax = (xmin*__w, xmax*__w, ymin*__h, ymax*__h)
        return (int(xmin), int(xmax), int(ymin), int(ymax))
    
    def __isTargetObject(self, __targetedObject:str, __labeledObject:str,):
        if __labeledObject == __targetedObject:
            return True
        return False

    def __makeRectAroundObject(self, __frame, __color, __recPostion:tuple[int,int,int,int]):
            cv2.rectangle(__frame, (__recPostion[0], __recPostion[2]), (__recPostion[1], __recPostion[3]), color=__color, thickness=1)

    def __LabelObject(self,__frame, __displayText:str, __color, __recPostion:tuple[int,int]):
            cv2.putText(__frame, __displayText, (__recPostion[0], __recPostion[2] - 10), cv2.FONT_HERSHEY_PLAIN, 1, __color, 2)