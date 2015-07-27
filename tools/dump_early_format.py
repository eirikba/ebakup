#!/usr/bin/env python3

import datetime
import hashlib
import re

class ParseError(Exception): pass
class MissingFeatureError(Exception): pass

def dump_main_file(inf, outf):
    helpers = Helpers(inf, outf)
    helpers.read_block_settings()
    inf.seek(0)
    data = inf.read(helpers.datasize + helpers.sumsize + 1)
    if len(data) != helpers.datasize + helpers.sumsize:
        raise ParseError('Main file did not contain exactly 1 data block')
    helpers.dump_settings_block(verify_type=b'ebakup database v1')

def dump_backup_file(inf, outf):
    helpers = Helpers(inf, outf)
    helpers.read_block_settings()
    helpers.dump_settings_block(verify_type=b'ebakup backup data')
    while not helpers.is_final_block_dumped:
        helpers.dump_backup_block()

class Helpers(object):
    def __init__(self, inf, outf):
        self.inf = inf
        self.outf = outf
        # Set to true whenever any block beyond end-of-file is
        # attempted dumped:
        self.is_final_block_dumped = False

    def read_block_settings(self):
        self.inf.seek(0)
        data = self.inf.read(10000)
        match = re.search(b'\nedb-blocksize:(\d+)\n', data)
        if not match:
            raise ParseError('Failed to find block size')
        blocksize = int(match.group(1))
        if match.start() >= blocksize:
            raise ParseError('Block size not in first block')
        match = re.search(b'\nedb-blocksum:([^\n]+)\n', data)
        if not match:
            raise ParseError('Failed to find block checksum')
        if match.start() >= blocksize:
            raise ParseError('Block checksum not in first block')
        if match.group(1) == b'sha256':
            self.sumalgo = hashlib.sha256
        else:
            raise MissingFeatureError(
                'Unknown checksum algorithm: ' + match.group(1).decode('utf-8'))
        self.sumsize = self.sumalgo().digest_size
        self.datasize = blocksize - self.sumsize

    def dump_settings_block(self, verify_type=None):
        self.inf.seek(0)
        data = self.inf.read(self.datasize)
        blocksum = self.inf.read(self.sumsize)
        if self.sumalgo(data).digest() != blocksum:
            raise ParseError('Non-matching checksum in settings block')
        end = data.find(b'\x00')
        if end >= 0:
            if data[end:].strip(b'\x00') != b'':
                raise ParseError('Trailing garbage in settings block')
        else:
            end = len(data)
        if data[end-1] != 10: # b'\n'
            raise ParseError('Last entry in first block does not end with LF')
        settings = data[:end-1].split(b'\n')
        if verify_type is not None and settings[0] != verify_type:
            raise ParseError(
                'Wrong type: ' + str(settings[0]) + ' vs ' + str(verify_type))
        self.outf.write(b'type: ' + settings[0] + b'\n')
        for setting in settings[1:]:
            key, value = setting.split(b':', 1)
            self.outf.write(b'setting: ' + key + b':' + value + b'\n')

    def dump_backup_block(self):
        data = self.inf.read(self.datasize)
        blocksum = self.inf.read(self.sumsize)
        if data == b'':
            self.is_final_block_dumped = True
            return
        if len(blocksum) != self.sumsize:
            raise ParseError('Short read (truncated file?)')
        if self.sumalgo(data).digest() != blocksum:
            raise ParseError('Non-matching block checksum')
        done = 0
        while done < len(data):
            if data[done] == 0:
                if data[done:].strip(b'\x00') != b'':
                    raise ParseError('Trailing garbage in backup block')
                return
            elif data[done] == 0x90:
                done += 1
                dirid, done = _parse_varuint(data, done)
                parent, done = _parse_varuint(data, done)
                namelen, done = _parse_varuint(data, done)
                name = data[done:done+namelen]
                done += namelen
                self.outf.write(
                    b'dir: (' + str(parent).encode('utf-8') + b'-' +
                    str(dirid).encode('utf-8') + b')')
                if b'\n' in name:
                    raise MissingFeatureError(
                        'LF in file names not implemented')
                self.outf.write(name)
                self.outf.write(b'\n')
            elif data[done] == 0x91:
                done += 1
                parent, done = _parse_varuint(data, done)
                namelen, done = _parse_varuint(data, done)
                name = data[done:done+namelen]
                done += namelen
                cidlen, done = _parse_varuint(data, done)
                cid = data[done:done+cidlen]
                done += cidlen
                size, done = _parse_varuint(data, done)
                mtime, nsec, done = _parse_mtime(data, done)
                self.outf.write(b'file: (' + str(parent).encode('utf-8') + b')')
                if b'\n' in name:
                    raise MissingFeatureError(
                        'LF in file names not implemented')
                self.outf.write(name)
                self.outf.write(b'\ncid: ')
                self.outf.write(b''.join(
                    '{:02x}'.format(x).encode('utf-8') for x in cid))
                self.outf.write(
                    b'\nsize: ' + str(size).encode('utf-8') + b'\nmtime: ')
                if mtime.microsecond != nsec // 1000:
                    # Should not happen. _parse_mtime should check this anyway.
                    raise ParseError(
                        'Last-modified time mismatch between '
                        'microsecond and nanosecond')
                self.outf.write(
                    str(mtime.replace(microsecond=0)).encode('utf-8'))
                if nsec != 0:
                    self.outf.write(b'.' + '{:09}'.format(nsec).encode('utf-8'))
                self.outf.write(b'\n')
            else:
                raise ParseError('Unknown data item type: ' + str(data[done]))

def _parse_varuint(data, done):
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

def _parse_mtime(data, done):
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
    month, day = _month_and_day_from_day_of_year(year, day)
    mtime = datetime.datetime(
        year, month, day, hour, minute, second, nsecs//1000)
    return mtime, nsecs, done + 9

daysofmonth = [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
def _month_and_day_from_day_of_year(year, day):
    leap_year= year % 400 == 0 or (year % 4 == 0 and year % 100 != 0)
    if not leap_year and day >= 59:
        # skip February 29
        day += 1
    for month, days in enumerate(daysofmonth):
        if day < days:
            return month+1, day+1
        day -= days
    assert False
