#!/usr/bin/env python3

# This module contains functions that transforms between values stored
# in the database and plain python values.

import datetime

def parse_uint32(data, done):
    return (data[done] + data[done+1] * 256 +
            data[done+2] * 0x10000 + data[done+3] * 0x1000000)

def make_uint32(value):
    if value < 0:
        raise ValueError('Can not make uint32 from negative number')
    if value > 0xffffffff:
        raise ValueError('Value too big for uint32: ' + str(value))
    return bytes((
        value & 0xff, (value >> 8) & 0xff,
        (value >> 16) & 0xff, (value >> 24) & 0xff))

def parse_varuint(data, done):
    if data[done] < 0x80:
        return data[done], done+1
    if data[done+1] < 0x80:
        return (data[done] & 0x7f) + (data[done+1] << 7), done+2
    if data[done+2] < 0x80:
        return (data[done] & 0x7f) + ((data[done+1] & 0x7f) << 7) + (data[done+2] << 14), done+3
    value = 0
    shift = 0
    while True:
        value += (data[done] & 0x7f) << shift
        shift += 7
        if data[done] < 0x80:
            return value, done + 1
        done += 1

def make_varuint(value):
    if value < 0:
        raise ValueError('Can not make varuint from negative number')
    if value == 0:
        return b'\x00'
    data = []
    while value > 0x7f:
        data.append((value & 0x7f) | 0x80)
        value >>= 7
    data.append(value)
    return bytes(data)

def parse_mtime(data, done):
    year = data[done] + (data[done+1] << 8)
    secs = (data[done+2] + (data[done+3] << 8) + (data[done+4] << 16) +
            ((data[done+5] & 0x80) << 17))
    nsecs = ((data[done+5] & 0x3f) + (data[done+6] << 6) +
             (data[done+7] << 14) + (data[done+8] << 22))
    assert year != 0 or (secs == 0 and nsecs == 0)
    assert nsecs >= 0
    assert nsecs < 1000000000
    day = secs // 86400
    assert day >= 0
    assert day < 366
    left = secs - day * 86400
    hour = left // 3600
    assert hour >= 0
    assert hour < 24
    left = left - hour * 3600
    minute = left // 60
    assert minute >= 0
    assert minute < 60
    second = left - minute * 60
    assert second >= 0
    assert second < 60
    month, day = month_and_day_from_day_of_year(year, day)
    mtime = datetime.datetime(
        year, month, day, hour, minute, second, nsecs//1000)
    return mtime, nsecs

daysofmonth = [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
def month_and_day_from_day_of_year(year, day):
    leap_year= year % 400 == 0 or (year % 4 == 0 and year % 100 != 0)
    if not leap_year and day >= 59:
        # skip February 29
        day += 1
    for month, days in enumerate(daysofmonth):
        if day < days:
            return month+1, day+1
        day -= days
    assert False

def make_mtime_with_nsec(mtime, nsec):
    assert mtime.microsecond == 0 or mtime.microsecond == nsec//1000
    if nsec < 0 or nsec >= 1000000000:
        raise ValueError('nsec out of range: ' + str(nsec))
    year = mtime.year
    assert year > 0
    assert year < 65536
    day = day_of_year_from_datetime(mtime)
    sec = day * 86400 + mtime.hour * 3600 + mtime.minute * 60 + mtime.second
    data = ( year & 0xff, year >> 8,
             sec & 0xff, (sec >> 8) & 0xff, (sec >> 16) & 0xff,
             ((sec >> 17) & 0x80) | (nsec & 0x3f),
             (nsec >> 6) & 0xff, (nsec >> 14) & 0xff, nsec >> 22 )
    return bytes(data)

def day_of_year_from_datetime(mtime):
    day = 0
    for month in range(mtime.month-1):
        day += daysofmonth[month]
    day += mtime.day
    year = mtime.year
    leap_year= year % 400 == 0 or (year % 4 == 0 and year % 100 != 0)
    if not leap_year and day >= 60:
        day -= 1
    return day - 1

def bytes_to_path_component(component):
    return component.decode('utf-8', errors='surrogateescape')

def path_component_to_bytes(component):
    if isinstance(component, bytes):
        return component
    return component.encode('utf-8', errors='surrogateescape')

