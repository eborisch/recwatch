#!/usr/bin/env python

# MIT License
# 
# Copyright (c) 2017 Mayo Clinic
# 
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
# 
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
# 
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

from __future__ import print_function

import datetime
import os
import subprocess
import sys
import threading

from collections import namedtuple
from datetime import datetime
from tempfile import TemporaryFile as TempFile
from time import time

from reclib.vars import DEBUG

_EMAIL = None

def set_email(newval = ''):
    global _EMAIL
    if newval is not '':
        _EMAIL = newval
    return _EMAIL


# Debug / logging functions
def logger(f):
    """Wrapper for call tracing at high debug levels."""
    if DEBUG > 4:
        def wrap(*args, **kwargs):
            tprint(0, "calling {}({},{})".format(f.__name__,
                                                 repr(args),
                                                 repr(kwargs)))
            return f(*args, **kwargs)
        return wrap
    else:
        return f


def c_logger(f):
    """Wrapper for class-method call tracing at high debug levels."""
    if DEBUG > 4:
        def wrap(self, *args, **kwargs):
            tprint(0, "calling {}.{}({},{})".format(self.__class__.__name__,
                                                    f.__name__,
                                                    repr(args),
                                                    repr(kwargs)))
            return f(self, *args, **kwargs)
        return wrap
    else:
        return f


def atomic_create(path, contents):
    # Creates a file at path and places contents into it, but only if the file
    # does not exist.
    afile = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL)
    os.write(afile, contents.encode('UTF-8'))
    os.fsync(afile)
    os.close(afile)
    subprocess.call("sync")


def stamp():
    """
    Returns a timestamp string for logging. (Includes thread ID if DEBUG > 1)
    """
    if DEBUG > 1:
        return "%s:[%d]" % (str(datetime.now()),
                            threading.currentThread().ident)
    return str(datetime.now())


def dprint(level, *args, **kwargs):
    """
    Conditional debug printing.
    """
    if DEBUG >= level:
        print(*args, **kwargs)


def tprint(level, *args, **kwargs):
    """
    Conditional time-stamped printing.
    """
    if 'file' in kwargs:
        f = kwargs['file']
    else:
        f = sys.stdout
    if DEBUG >= level:
        print(stamp() + ': ', file=f, end='')
        print(*args, **kwargs)


@logger
def mail_log(subject, message, attachment=None):
    """
    Send e-mail with a status message.
    """
    if _EMAIL is None:
        return
    try:
        tf = TempFile()
        tf.write(message.encode('UTF-8'))
        tf.seek(0, os.SEEK_SET)
        cmd = ["mail", "-s", subject]
        if attachment is not None and sys.platform[:5] == 'linux':
            cmd.extend(['-a', attachment])
        cmd.append(_EMAIL)
        subprocess.call(cmd, stdin=tf)
        tf.close()
    except Exception as e:
        tprint(0, "Unable to mail log!?!? [%s]" % str(e))


def with_lock(f):
    def wrap(self):
        with self._lock:
            ret = f(self)
        return ret
    return wrap


class TaskTimer(object):
    """
    Provides an elapsed timer with pause/resume support.
    Thread safe.
    """
    def __init__(self):
        self._lock = threading.Lock()
        with self._lock:
            self._elapsed_time = 0.0
            self._start_time = time()

    def __repr__(self):
        return '{}s [{}]'.format(self.seconds(),
                                 "paused" if self._is_paused() else "running")

    @with_lock
    @c_logger
    def pause(self):
        "Saves current elapsed time and stops clock."
        if self._is_paused():
            return
        self._elapsed_time = self._elapsed_time + self._lap()
        self._start_time = None

    @with_lock
    @c_logger
    def reset(self):
        "Restore initial state."
        self._elapsed_time = 0.0
        self._start_time = time()

    @with_lock
    @c_logger
    def resume(self):
        "Start clock again if paused."
        if not self._is_paused():
            return
        self._start_time = time()

    @with_lock
    @c_logger
    def seconds(self):
        "Seconds accrued."
        return self._elapsed_time + self._lap()

    @c_logger
    def _is_paused(self):
        "(Internal): True if paused. NO LOCK."
        return self._start_time is None

    @c_logger
    def _lap(self):
        "(Internal): Time on current running clock. NO LOCK."
        if self._is_paused():
            return 0
        else:
            return time() - self._start_time
