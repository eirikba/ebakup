#!/usr/bin/env python3

import datetime

class Logger(object):
    def __init__(self):
        self.raw_log = []

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
        self.raw_log.append(LogItem(severity, what, which, comment))

class LogItem(object):
    def __init__(self, severity, what, which, comment):
        self.when = datetime.datetime.utcnow()
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