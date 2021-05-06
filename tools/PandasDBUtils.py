import pandas as pd
import numpy as np

def dataFrameToSavableRecords(df, dataSet):
    saveRecords = []
    subset = df
    if dataSet:
        subset = df[dataSet]
    for subData in [tuple(x) for x in subset.values]:
        saveRecords.append(subData)
    return saveRecords


def nanToNone(df_data, columnList):
    for column in columnList:
        df_data.loc[:, (column)] = np.where(df_data[column].isna(), None, df_data[column])
    return df_data