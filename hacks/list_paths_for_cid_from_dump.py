#!/usr/bin/env python3

import argparse

import dumpreader


def main():
    args = parse_args()
    reader = dumpreader.DumpReader(args.dump)
    reader.add_filter(lambda x: x.cid == args.cid)
    reader.set_handler(lambda x: print_path(reader, x))
    reader.run()


def parse_args():
    parser = argparse.ArgumentParser(
        description='List all paths that matches a given cid in a dump file')
    parser.add_argument('dump', help='The dump file to search through')
    parser.add_argument('cid', help='The cid to search for')
    return parser.parse_args()


def print_path(reader, item):
    print(reader.path_of_item(item))



if __name__ == '__main__':
    main()
