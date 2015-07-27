#!/usr/bin/env python3

import argparse
import sys

import dump_early_format

def main():
    ap = argparse.ArgumentParser(
        'Dump the full content of an ebakup database file in a '
        'human-readable text format')
    ap.add_argument('file', help='Path to database file to be dumped')
    ap.add_argument(
        '--output', '-o',
        help='Path to output file (ANY EXISTING FILE WILL BE DESTROYED) '
        'default: print to standard output')
    args = ap.parse_args()
    dumper = get_dumping_function_for(args.file)
    if not dumper:
        print('Could not find dump function for ' + args.file, file=sys.stderr)
        return
    dump_file(args, dumper)
    print('Dump complete', file=sys.stderr)

def get_dumping_function_for(fn):
    with open(fn, 'rb') as f:
        return get_dumping_function_for_fileobj(f)

def get_dumping_function_for_fileobj(f):
    f.seek(0)
    data = f.read(100)
    if data.startswith(b'ebakup database v1'):
        return dump_early_format.dump_main_file
    elif data.startswith(b'ebakup backup data'):
        return dump_early_format.dump_backup_file
    return None

def dump_file(args, dump_func):
    with open(args.file, 'rb') as inf:
        outfn = args.output
        if outfn is None:
            outf = sys.stdout
        else:
            outf = open(outfn, 'wb')
        with outf:
            dump_fileobject(inf, outf, dump_func)

def dump_fileobject(inf, outf, dump_func):
    outf.write(b'event: dump start\n')
    dump_func(inf, outf)
    outf.write(b'event: dump complete\n')

if __name__ == '__main__':
    main()
