import pandas as pd


def load(path):
    df = pd.read_csv(path)
    return df

