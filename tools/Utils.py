import datetime
import time
import numpy as np

def getPreviousWorkingdayStr():
    currentDate = datetime.datetime.now()
    weekDay = currentDate.date().weekday()
    diff = 1
    if (weekDay == 5 or weekDay == 6):
        return
    elif (weekDay == 0):
        diff += 2
    runDay = currentDate - datetime.timedelta(days=diff)
    runDayStr = runDay.strftime('%Y-%m-%d')
    return runDayStr

def nanToNone(df_data, columnList):
    for column in columnList:
        df_data[column] = np.where(df_data[column].isna(), None, df_data[column])
    return df_data

def utc2local(utc_st):
    now_stamp = time.time()
    local_time = datetime.datetime.fromtimestamp(now_stamp)
    utc_time = datetime.datetime.utcfromtimestamp(now_stamp)
    offset = local_time - utc_time
    local_st = utc_st + offset
    return local_st