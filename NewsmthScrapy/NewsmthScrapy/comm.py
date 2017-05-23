from datetime import datetime, timedelta

def getYesterDay():
    yesterday= datetime.now()-timedelta(1)
    yesterday_str = yesterday.strftime("%Y-%m-%d")
    return yesterday_str