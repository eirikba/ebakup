#!/usr/bin/env python3

import os


root_path = os.path.abspath(os.path.join(os.getcwd(), 'DELETEME_testebakup'))


def makepath(*args):
    return os.path.join(root_path, *args)
