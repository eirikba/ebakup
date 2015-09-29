#!/usr/bin/env python3

import argparse
import sys

import dump_early_format

def main():
    ap = argparse.ArgumentParser(
        description='Dump the full content of an ebakup database file in a '
        'human-readable text format')
    ap.add_argument('file', help='Path to database file to be dumped')
    ap.add_argument(
        '--output', '-o',
        help='Path to output file (ANY EXISTING FILE WILL BE DESTROYED) '
        'default: print to standard output')
    args = ap.parse_args()
    with open(args.file, 'rb') as inf:
        if args.output is None:
            outf = sys.stdout
        else:
            outf = open(args.output, 'wb')
        with outf:
            dump_file_object(inf, outf)

def dump_file_object(inf, outf):
    dumper = get_dumping_function_for_file_object(inf)
    if not dumper:
        print('Could not find dump function', file=sys.stderr)
        return
    outf.write(b'event: dump start\n')
    dumper(inf, outf)
    outf.write(b'event: dump complete\n')

def get_dumping_function_for_file_object(f):
    f.seek(0)
    data = f.read(100)
    if data.startswith(b'ebakup database v1\n'):
        return dump_early_format.dump_main_file
    elif data.startswith(b'ebakup content data\n'):
        return dump_early_format.dump_content_file
    elif data.startswith(b'ebakup backup data\n'):
        return dump_early_format.dump_backup_file
    return None

if __name__ == '__main__':
    main()
