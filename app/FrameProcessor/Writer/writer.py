from abc import abstractmethod

import pandas as pd

from ..Utility.csv_loader import load


class IWriter:

    @abstractmethod
    def create_csv(self, header, path):
        pass

    @abstractmethod
    def write(self, frames, path):
        pass


class Writer(IWriter):

    def create_csv(self, header, path):
        df = pd.DataFrame(header)
        df.to_csv(path, index=False, header=header)

    def write(self, frames, path):
        df = load(path)
        df.to_csv(path, header=False, index=False, mode='a')
