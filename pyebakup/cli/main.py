#!/usr/bin/env python3

import argparse
import datetime
import os
import re
import sys
import time

import backupcollection
import backupoperation
import database
import filesys
import webui.http_handler as http_handler
import logger
import webui.ui_state as ui_state

from config import Config
from task_backup import BackupTask
from task_info import InfoTask
from task_makeshadowtree import MakeShadowTreeTask
from task_sync import SyncTask
from task_verify import VerifyTask
from task_webui import WebUITask

_start_time = datetime.datetime.utcnow()

class UnknownCommandError(Exception): pass

def main(commandline=None, stdoutfile=None, services=None):
    args = parse_commandline(commandline, stdoutfile)
    args.services = create_services(args, services)
    if stdoutfile is not None:
        args.services['logger'].set_outfile(stdoutfile)
    tasks = make_tasks_from_args(args)
    perform_tasks(tasks)
    if args.no_exit:
        while True:
            time.sleep(3600)


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
    parser.add_argument(
        '--no-exit', action='store_true',
        help='Do not exit when tasks are complete '
        '(e.g. to keep the web ui running)')
    parser.add_argument('--fake-start-time',
        help='Act as if this is the time when ebakup started '
            '(intended for testing)')
    subparsers = parser.add_subparsers(dest='command')
    backupparser = subparsers.add_parser(
        'backup', help='Perform one or more backup operations')
    backupparser.add_argument(
        '--create', action='store_true',
        help='Create the backup collection before starting the backup')
    backupparser.add_argument('backups', nargs='+', help='Which backups to run')
    infoparser = subparsers.add_parser(
        'info', help='Display information about the state of the backups')
    syncparser = subparsers.add_parser(
        'sync', help='Synchronize multiple collections for the same backup set')
    syncparser.add_argument(
        '--create', action='store_true',
        help='Create any missing backup collections')
    syncparser.add_argument(
        'backups', nargs='*', help='Which backups to sync (default:all)')
    verifyparser = subparsers.add_parser(
        'verify',
        help='Check whether the backed up data is consistent. This should '
        'detect most corruptions that may happen. '
        'WARNING: This is currently "work in progress" and does NOT '
        'fully check all that should be checked')
    shadowcopyparser = subparsers.add_parser(
        'shadowcopy', help='Makes a "shadow" copy of a snapshot.')
    shadowcopyparser.add_argument(
        '--target',
        help='The directory to create as the shadow copy. If the target '
        'already exists, the command will fail. If the target does not '
        'support having hardlinks created to files in the backup storage, '
        'the command will fail. This typically means it has to be on the '
        'same physical disk. WARNING: Do not modify the files in the '
        'shadow tree. That will also modify the originals in the backup!')
    shadowcopyparser.add_argument(
        'snapshotname', help='Name of snapshot to copy')
    webuiparser = subparsers.add_parser(
        'webui',
        help='Run the web ui. By default, the web ui is always started '
        'anyway. This command can be used to avoid giving any other command '
        'when only the web ui is needed. This command will also '
        'imply --no-exit.')
    if msgfile is not None:
        backupparser._overridden_output_file = msgfile
        infoparser._overridden_output_file = msgfile
        shadowcopyparser._overridden_output_file = msgfile
        webuiparser._overridden_output_file = msgfile
        syncparser._overridden_output_file = msgfile
        verifyparser._overridden_output_file = msgfile
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
    if args.command == 'webui':
        args.no_exit = True

def create_services(args, overrides):
    ui = ui_state.UIState(args)
    ui.set_http_handler(http_handler.HttpHandler)
    services = {
        'filesystem': filesys.get_file_system,
        'backupoperation': backupoperation.BackupOperation,
        'backupcollection.create': backupcollection.create_collection,
        'backupcollection.open': backupcollection.open_collection,
        'database.create': database.create_database,
        'database.open': database.Database,
        'utcnow': datetime.datetime.utcnow,
        'uistate': ui,
        'logger': True,
    }
    if args is not None and args.fake_start_time:
        _set_fake_start_time(args.fake_start_time)
        services['utcnow'] = _fake_utcnow
    if overrides is None:
        overrides={'*':None}
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
    if filtered.get('logger', True) is True:
        filtered['logger'] = logger.Logger(services=filtered)
    if filtered.get('utcnow'):
        ui.set_start_time(filtered['utcnow']())
    return filtered

def make_tasks_from_args(args):
    localtree = args.services['filesystem']('local')
    config = Config(args.services)
    if args.config:
        config.read_file(localtree, args.config)
    else:
        confpaths = localtree.get_config_paths_for('ebakup')
        for path in confpaths:
            config.read_file(localtree, path + ('config',))
    tasks = []
    tasks.append(WebUITask(config, args))
    if args.command == 'backup':
        task = BackupTask(config, args)
        tasks.append(task)
    elif args.command == 'info':
        task = InfoTask(config, args)
        tasks.append(task)
    elif args.command == 'shadowcopy':
        task = MakeShadowTreeTask(config, args)
        tasks.append(task)
    elif args.command == 'webui':
        pass
    elif args.command == 'sync':
        task = SyncTask(config, args)
        tasks.append(task)
    elif args.command == 'verify':
        task = _make_verify_task(config, args)
        tasks.append(task)
    else:
        raise UnknownCommandError('Unknown command: ' + args.command)
    return tasks

def _make_verify_task(config, args):
    cfgcollection = config.backups[0].collections[0]
    collection = args.services['backupcollection.open'](
        cfgcollection.filesystem, cfgcollection.path)
    task = VerifyTask(collection, services=args.services)
    task.printSummaryAfterCompletion()
    return task

def perform_tasks(tasks):
    for task in tasks:
        task.execute()

def _set_fake_start_time(start):
    global _fake_time_diff
    start_dt = _parse_datetime(start)
    _fake_time_diff = start_dt - _start_time

def _parse_datetime(timestr):
    match = re.match(
        r'^(\d{4})-(\d\d)-(\d\d)[T ](\d\d):(\d\d)(?::(\d\d)(?:\.(\d+)))$',
        timestr)
    if match is None:
        raise AssertionError('Failed parse datetime string: ' + str(timestr))
    year = int(match.group(1))
    month = int(match.group(2))
    day = int(match.group(3))
    hour = int(match.group(4))
    minute = int(match.group(5))
    secondstr = match.group(6)
    if secondstr:
        second = int(secondstr)
    else:
        second = 0
    subsecstr = match.group(7)
    if not subsecstr:
        micro = 0
    elif len(subsecstr) >= 6:
        micro = int(subsecstr[:6])
    else:
        micro = int(subsecstr) * 10 ** (6 - len(subsecstr))
    return datetime.datetime(year, month, day, hour, minute, second, micro)

def _fake_utcnow():
    return datetime.datetime.utcnow() + _fake_time_diff
