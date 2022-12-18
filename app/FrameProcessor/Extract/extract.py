from abc import abstractmethod
import re


class IExtract:
    @abstractmethod
    def extract(self, params, frames):
        pass

    @abstractmethod
    def extract_all(self, frame):
        pass

    @abstractmethod
    def frame_segmentation(self, frame: str):
        pass

    @abstractmethod
    def frame_time_stamp(self, number: int):
        pass


# 0666 Head.Left _ Leg.Down _ Wing.Off _ Tail.Center.jpg
class Extract(IExtract):
    __param: dict = {
        "time": 0,
        "head": 1,
        "leg": 2,
        "wing": 3,
        "tail": 4,
    }

    def __init__(self) -> None:
        pass
    
    def frame_time_stamp(self, number: int):
        hours = int(number / 108000)
        minutes = int(number / 1800) % 60
        seconds = int((number % 1800) / 24)
        # f = number % 1800 % 30
        return f"{hours}:{minutes}:{seconds}"

    def frame_segmentation(self, frame: str):
        # 0666Head.Left_Leg.Down_Wing.Off_Tail.Center.jpg
        frame = frame.replace(".jpg", "")  # Remove Extension

        # 0666Head.Left_Leg.Down_Wing.Off_Tail.Center
        frame_number = re.findall(r"\d+", frame)[0]

        frame = frame.replace(frame_number, "")
        # Head.Left_Leg.Down_Wing.Off_Tail.Center

        data = re.findall(r"((?<=\.)[a-zA-Z]+)", frame)
        data.insert(0, self.frame_time_stamp(int(frame_number)))
        # ['Left', 'Down', 'Off', 'Center']

        return data  # ['Left', 'Down', 'Off', 'Center']

    def extract(self, params, frame):
        segmented_frame = self.frame_segmentation(frame)
        try:
            selected_data = [segmented_frame[self.__param.get(x)] for x in params]
        except:
            print(frame)
        return selected_data

    def extract_all(self, frame):
        segmented_frame = self.frame_segmentation(frame)
        return segmented_frame

