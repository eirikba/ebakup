#!/usr/bin/env python3

import argparse
import datetime
import os
import sys

import backupcollection
import backupoperation
import database
import filesys
import logger

from config import Config
from task_backup import BackupTask
from task_info import InfoTask

class UnknownCommandError(Exception): pass

def main(commandline=None, stdoutfile=None, services=None):
    args = parse_commandline(commandline, stdoutfile)
    args.services = create_services(services)
    args.logger = logger.Logger()
    if stdoutfile is not None:
        args.logger.set_outfile(stdoutfile)
    tasks = make_tasks_from_args(args)
    perform_tasks(tasks)

class ArgumentParser(argparse.ArgumentParser):
    _overridden_output_file = None
    def _print_message(self, msg, outfile):
        if self._overridden_output_file is not None:
            outfile = self._overridden_output_file
        argparse.ArgumentParser._print_message(self, msg, outfile)

def parse_commandline(commandline=None, msgfile=None):
    parser = ArgumentParser(
        description='Manage backups')
    if msgfile is not None:
        parser._overridden_output_file = msgfile
    parser.add_argument(
        '--config', help='Config file to use instead of the default')
    subparsers = parser.add_subparsers(dest='command')
    backupparser = subparsers.add_parser(
        'backup', help='Perform one or more backup operations')
    backupparser.add_argument(
        '--create', action='store_true',
        help='Create the backup collection before starting the backup')
    backupparser.add_argument('backups', nargs='+', help='Which backups to run')
    infoparser = subparsers.add_parser(
        'info', help='Display information about the state of the backups')
    if msgfile is not None:
        backupparser._overridden_output_file = msgfile
        infoparser._overridden_output_file = msgfile
    if commandline is None:
        args = parser.parse_args()
    else:
        args = parser.parse_args(commandline)
    if args.command is None:
        parser.print_usage()
        sys.exit(1)
    _fixup_arguments(args)
    return args

def _fixup_arguments(args):
    if args.config is not None:
        localfs = filesys.get_file_system('local')
        args.config = localfs.path_from_string(args.config)

def create_services(overrides):
    services = {
        'filesystem': filesys.get_file_system,
        'backupoperation': backupoperation.BackupOperation,
        'backupcollection.create': backupcollection.create_collection,
        'backupcollection.open': backupcollection.open_collection,
        'database.create': database.create_database,
        'database.open': database.Database,
        'utcnow': datetime.datetime.utcnow,
    }
    if overrides is None:
        return services
    filtered = {}
    for key, value in overrides.items():
        if key == '*':
            continue
        assert key in services
        if value is None:
            filtered[key] = services[key]
        else:
            filtered[key] = value
    if '*' in overrides:
        if overrides['*'] is None:
            for key,value in services.items():
                if key not in filtered:
                    filtered[key] = value
        else:
            raise AssertionError('overrides["*"] has unexpected value')
    return filtered

def make_tasks_from_args(args):
    localtree = args.services['filesystem']('local')
    config = Config()
    if args.config:
        config.read_file(localtree, args.config)
    else:
        confpaths = localtree.get_config_paths_for('ebakup')
        for path in confpaths:
            config.read_file(localtree, path + ('config',))
    tasks = []
    if args.command == 'backup':
        task = BackupTask(config, args)
        tasks.append(task)
    elif args.command == 'info':
        task = InfoTask(config, args)
        tasks.append(task)
    else:
        raise UnknownCommandError('Unknown command: ' + args.command)
    return tasks

def perform_tasks(tasks):
    for task in tasks:
        task.execute()
