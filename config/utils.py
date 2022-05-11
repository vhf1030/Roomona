from datetime import datetime, timedelta


def date_convert(date_, delta=0):
    if type(date_) == str:
        dtime = datetime.strptime(date_, '%Y-%m-%d')
    if type(date_) == datetime:
        dtime = date_
    dtime += timedelta(days=delta)
    res_str = str(dtime).split(' ')[0]
    return res_str
