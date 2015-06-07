#!/usr/bin/env python3

import datetime

class Logger(object):
    def __init__(self):
        self.raw_log = []
        self._utcnow = datetime.datetime.utcnow
        self._outfile = None

    def set_utcnow(self, utcnow):
        self._utcnow = utcnow

    def set_outfile(self, outfile):
        '''The file object where direct output is sent. Defaults to standard
        output.
        '''
        self._outfile = outfile

    # Log severities.
    # DO NOT DEPEND ON NUMERICAL VALUES!
    # But you can depend on "more critical" severities having higher
    # numerical values than "less critical" severities.
    LOG_DEBUG2 = -1 # Drowning in detailed information
    LOG_DEBUG = 0 # Only interesting for debugging the application
    LOG_INFO = 1 # Expected events that might be worth knowing about
    LOG_NOTICE = 2 # It may be that something is wrong
    LOG_WARNING = 3 # It is reasonable to think that something is wrong
    LOG_ERROR = 4 # Something is almost surely wrong!
    LOG_CRITICAL = 5 # Not only is it wrong, but it causes other
                     # important things to fail!
    def log(self, severity, what, which, comment=''):
        '''Add an event to the log.

        'severity' is one of the LOG_* values, giving an indication of
        how serious the event is. 'what' is a string identifying the
        class of events, defined by the caller. 'which' is the
        object(s) the event applies to. And 'comment' is a string
        providing further details about this specific event, intended
        for human consumption.
        '''
        self.raw_log.append(
            LogItem(self._utcnow(), severity, what, which, comment))

    def log_error(self, what, which, comment=''):
        self.log(self.LOG_ERROR, what, which, comment)

    def replay_log(self, receiver):
        receiver.raw_log += self.raw_log

    def print(self, msg):
        '''Display 'msg' to the user.
        '''
        if self._outfile is None:
            print(msg)
        else:
            print(msg, file=self._outfile)

class LogItem(object):
    def __init__(self, when, severity, what, which, comment):
        self.when = when
        self.severity = severity
        self.what = what
        self.which = which
        self.comment = comment

    def __str__(self):
        string = self.what + ' - ' + str(self.which)
        if self.comment:
            string += ': ' + self.comment
        return string

class NoLogger(object):
    '''A Logger that discards all the logs.
    '''
    def log(self, severity, what, which, comment=''):
        pass
