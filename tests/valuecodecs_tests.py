#!/usr/bin/env python3

import datetime
import unittest

import valuecodecs

class TestCodecs(unittest.TestCase):
    def test_mtime_to_db_codec(self):
        encode = valuecodecs.make_mtime_with_nsec
        decode = lambda x: valuecodecs.parse_mtime(x, 0)
        pairs = (
            (b'\x01\x00\x00\x00\x00\x00\x00\x00\x00',
             datetime.datetime(1, 1, 1, 0, 0, 0), 0),
            (b'\x01\x00\x01\x00\x00\x00\x00\x00\x00',
             datetime.datetime(1, 1, 1, 0, 0, 1), 0),
            (b'\x01\x00\x3c\x00\x00\x00\x00\x00\x00',
             datetime.datetime(1, 1, 1, 0, 1, 0), 0),
            (b'\x01\x00\x10\x0e\x00\x00\x00\x00\x00',
             datetime.datetime(1, 1, 1, 1, 0, 0), 0),
            (b'\x01\x00\x80\x51\x01\x00\x00\x00\x00',
             datetime.datetime(1, 1, 2, 0, 0, 0), 0),
            (b'\x01\x00\x00\x8d\x27\x00\x00\x00\x00',
             datetime.datetime(1, 1, 31, 0, 0, 0), 0),
            (b'\x01\x00\x80\xde\x28\x00\x00\x00\x00',
             datetime.datetime(1, 2, 1, 0, 0, 0), 0),
            (b'\x01\x00\x80\x25\x4b\x00\x00\x00\x00',
             datetime.datetime(1, 2, 27, 0, 0, 0), 0),
            (b'\x01\x00\x00\x77\x4c\x00\x00\x00\x00',
             datetime.datetime(1, 2, 28, 0, 0, 0), 0),
            (b'\x01\x00\x80\xc8\x4d\x00\x00\x00\x00',
             datetime.datetime(1, 3, 1, 0, 0, 0), 0),
            (b'\x01\x00\x00\x00\x00\x01\x00\x00\x00',
             datetime.datetime(1, 1, 1, 0, 0, 0), 1),
            (b'\x01\x00\x00\x00\x00\x3f\x00\x00\x00',
             datetime.datetime(1, 1, 1, 0, 0, 0), 63),
            (b'\x01\x00\x00\x00\x00\x00\x01\x00\x00',
             datetime.datetime(1, 1, 1, 0, 0, 0), 64),
            (b'\x01\x00\x00\x00\x00\x00\x20\x00\x00',
             datetime.datetime(1, 1, 1, 0, 0, 0, (1<<11) // 1000), 1<<11),
            (b'\x01\x00\x00\x00\x00\x00\x80\x00\x00',
             datetime.datetime(1, 1, 1, 0, 0, 0, (1<<13) // 1000), 1<<13),
            (b'\x01\x00\x00\x00\x00\x00\x00\x01\x00',
             datetime.datetime(1, 1, 1, 0, 0, 0, (1<<14) // 1000), 1<<14),
            (b'\x01\x00\x00\x00\x00\x00\x00\x80\x00',
             datetime.datetime(1, 1, 1, 0, 0, 0, (1<<21) // 1000), 1<<21),
            (b'\x01\x00\x00\x00\x00\x00\x00\x00\x01',
             datetime.datetime(1, 1, 1, 0, 0, 0, (1<<22) // 1000), 1<<22),
            (b'\x01\x00\x00\x00\x00\x00\x00\x00\x40',
             datetime.datetime(1, 1, 1, 0, 0, 0, (1<<28) // 1000), 1<<28),
            (b'\x01\x00\x00\x00\x00\x00\x00\x00\x80',
             datetime.datetime(1, 1, 1, 0, 0, 0, (1<<29) // 1000), 1<<29),
        )
        for pair in pairs:
            self.assertEqual(pair[0], encode(pair[1], pair[2]))
            self.assertEqual((pair[1], pair[2]), decode(pair[0]))
        mtime = datetime.datetime(2015, 1, 1, 12, 42, 18)
        mtime2 = datetime.datetime(2015, 1, 1, 12, 42, 18, 249778)
        nsec = 249778391
        encoded = encode(mtime, nsec)
        self.assertEqual((mtime2, nsec), decode(encoded))
        encoded = encode(mtime2, nsec)
        self.assertEqual((mtime2, nsec), decode(encoded))
        mtime = datetime.datetime(2012, 1, 1, 12, 42, 18)
        mtime2 = datetime.datetime(2012, 1, 1, 12, 42, 18, 249778)
        nsec = 249778391
        encoded = encode(mtime, nsec)
        self.assertEqual((mtime2, nsec), decode(encoded))
        mtime = datetime.datetime(2015, 6, 6, 12, 42, 18)
        mtime2 = datetime.datetime(2015, 6, 6, 12, 42, 18, 249778)
        nsec = 249778391
        encoded = encode(mtime, nsec)
        self.assertEqual((mtime2, nsec), decode(encoded))
        mtime = datetime.datetime(2012, 6, 6, 12, 42, 18)
        mtime2 = datetime.datetime(2012, 6, 6, 12, 42, 18, 249778)
        nsec = 249778391
        encoded = encode(mtime, nsec)
        self.assertEqual((mtime2, nsec), decode(encoded))

