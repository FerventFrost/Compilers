import os
from abc import abstractmethod


class ILoader:
    @abstractmethod
    def load(self, folder_path):
        return


class Loader(ILoader):
    images = []
    
    def __init__(self) -> None:
        pass
    
    def load(self, folder_path):
        for filename in os.listdir(folder_path):
            self.images.append(filename)
        return self.images
