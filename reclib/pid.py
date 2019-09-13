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

import os
import platform
import signal
import subprocess
import threading

from collections import namedtuple

from reclib.util import tprint, dprint, logger, stamp

# ACTIVE_LOCK is used to control access to the ACTIVE, PAUSED, WAITING, and
# _run_level so two different threads don't think that it's their turn to
# start / pause / resume things.
ACTIVE_LOCK = threading.Condition()

_run_level = 20

def run_level():
    return _run_level

# ACTIVE and PAUSED are 20-length (# of priorities; 0..19) lists storing either
# None (no task at that level) or a PidInfo-tuple describing a launched task.
ACTIVE = [None] * 20
PAUSED = [None] * 20
# WAITING is a 20-length list containing None or True (for levels where a
# queued task is currently waiting on ACTIVE_LOCK to start.
WAITING = [None] * 20

# Types
# PidInfo objects are stored in ACTIVE[] and PAUSED[] to describing running
# tasks.
PidInfo = namedtuple('PidInfo',
                     ('pid', 'timer', 'name'))

def get_populated(X):
    """
    Convenience function to return only the populated portion of ACTIVE,
    PAUSED, or WAITING -- passed as a string. Returns generator of [(priority,
    item)].
    """
    if X == 'ACTIVE':
        X = ACTIVE
    elif X == 'PAUSED':
        X = PAUSED
    elif X == 'WAITING':
        X = WAITING
    else:
        raise Exception("Requested queue is not ACTIVE, PAUSED, or WAITING.")

    return [(n, X[n]) for n in range(len(X)) if X[n] is not None]

# PID control functions

@logger
def add_pid(pid, priority, timer, name):
    """
    Adds the given pid with associated timer and name at the provided priority
    level to the ACTIVE[] list.
    """
    with ACTIVE_LOCK:
        if ACTIVE[priority] is not None:
            # Logic is intended to prevent this from happening.
            dprint(-1, "PID COLLISION!!!")
        ACTIVE[priority] = PidInfo(pid, timer, name)


@logger
def discard_pid(pid, priority):
    """
    Removes the given pid at the specified priority level from either the
    ACTIVE or PAUSED list.
    """
    with ACTIVE_LOCK:
        # There is a chance someone has tried to pause us right as we were
        # finishing up. Check ACTIVE and PAUSED.
        for s in (ACTIVE, PAUSED):
            if s[priority] is not None and s[priority].pid == pid:
                s[priority] = None
                return
    tprint(0, "Unable to find PID?")


@logger
def kill_pid_tree(pid, sig=signal.SIGKILL):
    """
    Signals pid and pid's children with sig. Raises error on missing (finished)
    pid, but suppresses errors on children.
    """
    dprint(3, "Sending signal %s to %s" % (sig, pid))
    os.killpg(os.getpgid(pid), sig)
    for c in find_children_pgids(pid):
        try:
            dprint(3, "Sending signal %s to %s" % (sig, c))
            os.killpg(c, sig)
        except:  # Don't fail on kids
            pass


@logger
def find_children_pids(pid_in, ps_out=None):
    """
    Tries to locate the PIDs of all children recursively) of pid_in.
    ps_out is used when called recursively to avoid extra system calls.
    """
    if ps_out is None:
        if 'CYGWIN' in platform.system():
            ps_out = subprocess.check_output(
                       "ps -f | awk 'NR==1 {next} /^[^S]/{print $3\" \"$2}'",
                       shell=True, universal_newlines=True)
        else:
            ps_out = subprocess.check_output(('ps', '-A', '-oppid=', '-opid='),
                                             universal_newlines=True)
        ps_out = [x.strip().split() for x in ps_out.split('\n') if len(x)]
        ps_out = [(int(x[0]), int(x[1])) for x in ps_out]
        # Each element is now (parentpid, pid)
    pids = set()
    for x in ps_out:
        if x[0] == pid_in and x[1] != pid_in:
            pids.add(x[1])
    dprint(2, pids)
    for p in pids.copy():
        pids.update(find_children_pids(p, ps_out))
    return pids


@logger
def find_children_pgids(pid_in):
    """
    Tries to find all pgids of children of pid_in.
    """
    return set([os.getpgid(pid) for pid in find_children_pids(pid_in)])


@logger
def freeze_tasks(priority):
    """
    Freeze tasks less than the given priority. *(Greater in numeric value)
    """
    global _run_level
    with ACTIVE_LOCK:
        if priority < _run_level:
            _run_level = priority
        for p in range(priority, 20):
            if ACTIVE[p] is None:
                continue
            pinfo = ACTIVE[p]
            pid = pinfo.pid
            dprint(0, "%s: Pausing: [%s q:%d]" % (stamp(), pinfo.name, p))
            try:
                kill_pid_tree(pid, signal.SIGSTOP)
                # If kill_pid_tree raised an error (main PID gone) we won't
                # add it to PAUSED[] here; still removed from ACTIVE[] below.
                PAUSED[p] = pinfo
            except OSError as e:
                if e.errno != 3:
                    dprint(1, "%s: Unable to pause: [%s]" % (stamp(),
                                                             str(e)))
            # No longer exists or paused
            pinfo.timer.pause()
            ACTIVE[p] = None


@logger
def thaw_tasks():
    """
    Resumes tasks (but only the set with highest priority.)
    """
    # Want to find highest priority (lowest value [0..19]) paused job
    global _run_level
    with ACTIVE_LOCK:
        for rl in range(20):
            # Find highest priority paused or waiting job
            if PAUSED[rl] is not None:
                p = rl
                _run_level = rl
                break
            if WAITING[rl] is not None:
                p = None
                _run_level = rl
                break
        else:
            p = None
            _run_level = 20

        if p is not None:  # We've found a paused job to resume
            pinfo = PAUSED[p]
            pid = pinfo.pid
            dprint(0, "%s: Resuming: [%s q:%d]" % (stamp(),
                                                   pinfo.name,
                                                   p))
            try:
                kill_pid_tree(pid, signal.SIGCONT)
                # If kill_pid_tree raised an error (main PID gone) we won't
                # add it to ACTIVE[] here; still removed from PAUSED[] below.
                ACTIVE[p] = pinfo
                pinfo.timer.resume()
            except OSError as e:
                if e.errno != 3:
                    dprint(0, "%s: Unable to resume: [%s]" % (stamp(), str(e)))
            PAUSED[p] = None
        ACTIVE_LOCK.notify_all()

# vim: et:ts=4:sw=4:si:ai
