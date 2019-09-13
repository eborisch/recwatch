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

try:
    # Python 3
    import queue
except ImportError:
    # Python 2
    import Queue as queue

import os
import re
import shlex
import subprocess
import sys
import threading
import traceback

from collections import defaultdict
from contextlib import contextmanager
from datetime import datetime
from fnmatch import filter as fnfilter
from shutil import rmtree
from stat import S_IRUSR, S_IRGRP
from textwrap import dedent
from time import sleep

from reclib.util import TaskTimer, c_logger, stamp, DEBUG, tprint, mail_log
from reclib.pid import thaw_tasks, discard_pid, add_pid, kill_pid_tree, \
                       ACTIVE_LOCK, run_level, WAITING, freeze_tasks

pjoin = os.path.join

LOGPATH = None

def set_logpath(newpath):
    global LOGPATH
    if not os.path.exists(newpath):
        os.makedirs(newpath)
    LOGPATH=newpath

def copytree(A, B, copywhen, exclude=('.origin', '.work', 'latest')):
    """
    Copies (recursively) A to B via hard links (when possible), symlinks (copy
    symlinks as symlinks) and recreating directory structure.

    copywhen() is passed a file path, and returns true (copy) or false to
    determine which files should be copied.

    Excludes files / directories with names in the dictionary exclude.
    """

    # Handle case of trailing slash on A or B
    if A[-1] == '/':
        A = A[:-1]
    if B[-1] == '/':
        B = B[:-1]

    # Clean up any existing output tree
    if os.path.exists(B):
        rmtree(B)
    os.mkdir(B)

    # length of A with trailing slash
    prefix_len = len(A) + 1
    for (root, dirs, files) in os.walk(A):
        # Don't go into work directory itself
        for omit in exclude:
            if omit in dirs:
                dirs.remove(omit)
            if omit in files:
                files.remove(omit)

        rel = root[prefix_len:]

        syms = []

        # link files at this level
        for f in files:
            fn = pjoin(root, f)
            if copywhen(fn):
                # Only link in original files / directories
                if os.path.islink(fn):
                    syms.append(f)
                else:
                    os.link(fn, pjoin(B, rel, f))

        for d in dirs:
            dn = pjoin(root, d)
            if copywhen(dn):
                # Only link in original files / directories
                if os.path.islink(dn):
                    syms.append(d)
                else:
                    os.mkdir(pjoin(B, rel, d))

        for s in syms:
            sn = pjoin(root, s)
            tgt = os.readlink(sn)
            if '../' in tgt:
                # Relative links that don't go up we can copy directly
                tgt = pjoin(os.path.dirname(sn), os.readlink(sn))
            os.symlink(tgt, pjoin(B, rel, s))

class JobDescription(object):
    """
    Manages a particular job definition. Has work added to its work queue
    (by main thread) with self.enqueue(), and work performed (in a separate
    thread) by self.process_dir()
    """
    queues = defaultdict(queue.Queue)

    def __init__(self,
                 name,
                 files=tuple(),
                 pre_command=None,
                 post_command="touch done",
                 command=None,
                 priority=0,
                 timeout=3600,
                 isolate=False):
        # Constants; set at creation
        self.NAME = name
        self.FILES = files
        self.PRE_COMMAND = pre_command
        self.POST_COMMAND = post_command
        self.COMMAND = command
        self.PRIORITY = int(max(min(19, priority), 0))
        self.TIMEOUT = timeout
        self.ISOLATE = isolate

        command_error = False
        if type(self.COMMAND) is tuple:
            for n in self.COMMAND:
                if type(n) is not str:
                    command_error = True
        elif type(self.COMMAND) is not str:
            command_error = True

        if command_error:
            raise TypeError("'command' argument must be a string or a tuple"
                            " of strings.")

        # State variables; set when processing
        self.log = None
        self.thread = None
        self.job_dir = None
        self.files_found = None
        self.completed = 0
        self.timer = TaskTimer()

    def __del__(self):
        self._reset()

    @c_logger
    def _jpath(self, name):
        return pjoin(self.job_dir, name)

    @c_logger
    def enqueue(self, job_dir):
        """
        Called from main thread to request this job to be run in the
        given job_dir.
        """
        self.hprint(0, "Enqueuing [%s]" % (job_dir))
        JobDescription.queues[self.PRIORITY].put((self, job_dir, TaskTimer()))

    @c_logger
    def _isolate(self):
        """
        If self.ISOLATE is not set, does nothing.

        Copies (hard-links when possible) files older than job_dir/.origin from
        job_dir into job_dir/.work

        Creates job_dir/.origin if it doesn't exist.
        Creates job_dir/.stamp to record start of recon time
        Sets job_dir to job_dir/.origin
        """
        if not self.ISOLATE:
            return

        origin_file = self._jpath('.origin')
        if not os.path.exists(origin_file):
            with open(origin_file, 'w'):
                pass
            birth = os.stat(origin_file).st_mtime
            # If we make an origin file, wait until it is clearly "old"
            if birth % 1 > 0.0:
                sleep(.01)
            else:
                sleep(1)
        else:
            birth = os.stat(origin_file).st_mtime

        # As we are the first thing called on a new job directory, everything
        # older than .origin is an "input".

        work_dir = self._jpath('.work')

        copytree(self.job_dir, work_dir,
                 copywhen=lambda f: os.lstat(f).st_mtime <= birth)

        # Make sure the .stamp file is clearly "newer" than the "input"
        if birth % 1 > 0.0:
            sleep(.01)
        else:
            sleep(1)

        stamp_file = self._jpath('.stamp')
        with open(stamp_file, 'w') as sfile:
            # Make sure it is modified if it was somehow left around
            sfile.write('.')

        self.job_dir = work_dir

    def _harvest(self):
        """
        If self.ISOLATE is not set, does nothing.

        Restores job_dir to original (../) job directory

        Copies (hard link / effectively move when possible) newly created
        output files from .job_dir/.work into job_dir/Run_<stamp> and creates
        latest->Run_<stamp> symlink

        Cleans up .job_dir/(.work|.stamp)

        Moves recon.log into Run_<stamp>/
        """
        if not self.ISOLATE:
            return

        self.job_dir = os.path.dirname(self.job_dir)

        artifacts = datetime.now().strftime('Run_%Y_%m_%d_%H%M%S')
        artifacts = self._jpath(artifacts)
        artifacts_base = artifacts
        n_exist = 1
        while os.path.exists(artifacts):
            artifacts = artifacts_base + '.{}'.format(n_exist)
            n_exist = n_exist + 1

        os.mkdir(artifacts)

        stamp_file = self._jpath('.stamp')
        start_time = os.stat(stamp_file).st_mtime

        copytree(self._jpath('.work'), artifacts,
                 copywhen=lambda f: os.lstat(f).st_mtime >= start_time)

        latest = self._jpath('latest')

        if os.path.lexists(latest):
            os.unlink(latest)

        os.symlink(os.path.basename(artifacts), latest)

        os.unlink(stamp_file)
        rmtree(self._jpath('.work'), ignore_errors=True)

        # Leave logs in separate logpath if set.
        if LOGPATH is None:
            os.rename(self._jpath('recon.log'),
                      os.path.join(artifacts, 'recon.log'))

    @c_logger
    @contextmanager
    def _isolation(self):
        """
        Handles entering and exiting isolation. Effectively does nothing
        if self.ISOLATE is not set.
        """
        self._isolate()
        try: 
            yield
        finally:
            self._harvest()

    @c_logger
    @contextmanager
    def _run_dir(self, directory):
        """
        Wraps running of the actual tasks for a directory; ensures
        cleanup of local attributes for job completion.
        """
        self.job_dir = directory
        try:
            yield
        except Exception as err:
            self.hprint(0, "Errored while running")
            # traceback.print_exception(*sys.exc_info())
            if not os.path.exists(directory):
                self.hprint(0, "Directory disappeared between enqueue and run!")
            raise err
        finally:
            thaw_tasks()
            # We need to read out the timer value outside this context
            self._reset(reset_timer=False)

    @c_logger
    def process_dir(self, directory):
        """Convenience method to set directory and run all phases."""
        if self.job_dir is not None:
            self.hprint(0,
                        "Job directory is already set?")
            return 1

        self.timer.reset()
        # Ensure that even if one of pre/run/post fails we still
        # isolate/harvest
        with self._run_dir(directory), self._isolation():
            self._pre()
            self._run()
            self._post()

        t = self.timer.seconds()
        self.timer.reset()
        return t

    def header(self):
        """
        Logging header.
        """
        if self.job_dir:
            return "%s: [%s:%d] in [%s]: " % (stamp(),
                                              self.NAME,
                                              self.completed,
                                              self.job_dir)
        else:
            return "%s: [%s]: " % (stamp(), self.NAME)

    def hprint(self, n, *args, **kwargs):
        """
        Print (if DEBUG high enough) with logging header.
        """
        if n > DEBUG:
            return
        if 'file' in kwargs:
            f = kwargs['file']
        else:
            f = sys.stdout
        print(self.header(), file=f, end='')
        print(*args, **kwargs)

    @c_logger
    def _check_files(self):
        """
        Wait for 5s for files to show up.
        """
        # If we've already found the files, there is no need to search again.
        if self.files_found is not None:
            return

        def jpath(x):
            return pjoin(self.job_dir, x)

        tries = 5
        while 1:
            try:
                files_found = [''] * len(self.FILES)
                dlist = os.listdir(self.job_dir)
                for f_n in range(len(self.FILES)):
                    f = self.FILES[f_n]
                    found = fnfilter(dlist, f)
                    if len(found) == 0:
                        self.hprint(0, "Unable to find [%s]!" % f,
                                    file=sys.stderr)
                        raise IOError("Unable to find [%s] in [%s]!" %
                                      (f, self.job_dir))
                    if len(found) > 1:
                        self.hprint(0, "More than one match for [%s]!?" % f)
                    files_found[f_n] = found[0]
                    os.chmod(jpath(found[0]), S_IRUSR | S_IRGRP)
                # Only update state once we find them all.
                self.files_found = tuple(files_found)
                self.hprint(2, "Found files: " + repr(files_found))
                break
            except IOError as e:
                self.hprint(-1, "Error while finding files: [%s]" % str(e),
                            file=sys.stderr)
                if tries <= 0:
                    raise
                tries = tries - 1
                sleep(1)

        # We have found files. do md5 check now.
        md5file = fnfilter(dlist, "*.md5")
        if len(md5file) == 0:
            return
        md5file = md5file[0]
        s = subprocess.Popen(['md5sum', '-c', md5file], cwd=self.job_dir,
                             stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE, close_fds=True,
                             universal_newlines=True)
        csumout = s.communicate()

        msg = "# %s: [%s] MD5 check %%s\n" % (stamp(), self.NAME)

        if s.returncode == 0:
            with open(jpath('md5_tmp'), 'w') as md5_tmp:
                md5_tmp.write(csumout[0])
            os.rename(jpath('md5_tmp'), jpath('md5_done'))
            self.log.write(msg % "succeeded")
            self.hprint(0, "MD5 check succeeded")
        else:
            with open(jpath('md5_bad'), 'w') as md5_bad:
                md5_bad.write(csumout[0])
            self.log.write(msg % "FAILED")
            self.hprint(0, "MD5 check FAILED")

        [self.log.write("# %s\n" % l) for l in csumout[0].split('\n')]

        if s.returncode != 0:
            raise RuntimeError("Error in checksum testing. [%s]" %
                               csumout[1].strip())

    @c_logger
    def _pre(self):
        """
        Run single PRE command.
        """
        self._try_command(command=self.PRE_COMMAND, stage='PRE')

    @c_logger
    def _run(self):
        """
        Run MAIN; supports tuple of commands to run.
        """
        if isinstance(self.COMMAND, str):
            self._try_command(command=self.COMMAND, stage='MAIN')

        else:
            for n in range(len(self.COMMAND)):
                self._try_command(command=self.COMMAND[n],
                                  stage='MAIN[%d]' % n)

    @c_logger
    def _post(self):
        """
        Run single POST command.
        """
        self._try_command(command=self.POST_COMMAND, stage='POST')

    @c_logger
    def _reset(self, reset_timer=True):
        """
        Used in process_dir() to reset for next execution of this
        JobDescription.
        """
        if isinstance(self.log, file):
            self.log.write("# %s: Processing completed.\n" %
                           (stamp()))
            self.log.flush()
            self.log.close()
            self.completed = self.completed + 1
        self.log = None
        self.job_dir = None
        self.files_found = None
        if reset_timer:
            self.timer.reset()

    @c_logger
    def _try_command(self, command, stage):
        """
        Wrapper around stage execution. Catch any unhandled exceptions here.
        """

        LOG_MESSAGE = dedent("""\
            Error during [{0}] run in [{1}]:
            {2}

            Occurred in stage {3}; log attached.

            Failed command: [{4}]

            Backtrace:
            {5}
            """)

        if command is None:
            return 0

        self.hprint(2, "Running " + stage)
        try:
            self._run_command(command, stage)
        except Exception as e:
            self.hprint(-1, "Error processing stage %s : %s" % (stage, str(e)),
                        file=sys.stderr)
            self.hprint(-1, "Backtrace follows.", file=sys.stderr)
            TRACE = ''.join(traceback.format_exception(*sys.exc_info()))
            print(TRACE)

            mail_log("{0}: Error processing stage {1}".format(self.NAME,
                                                              stage),
                     LOG_MESSAGE.format(self.NAME,
                                        self.job_dir,
                                        str(e),
                                        stage,
                                        re.sub("  +", " ", command),
                                        TRACE),
                     attachment=self.log.name)
            raise

    @c_logger
    def _prep_log(self):
        """
        Open the log if we haven't already. Returns True if a new log was
        opened.
        """
        if self.log is not None:
            return False

        if LOGPATH is None:
            LOG_BASE = pjoin(self.job_dir,
                             '../recon.log' if self.ISOLATE else 'recon.log')
        else:
            # job_dir is normalized to remove trailing '/'s
            if self.ISOLATE:
                jname = os.path.basename(os.path.dirname(self.job_dir))
            else:
                jname = os.path.basename(self.job_dir)
            logd = pjoin(LOGPATH, self.NAME, jname)
            if not os.path.exists(logd):
                os.makedirs(logd)
            LOG_BASE = pjoin(logd, 'recon.log')

        LOG_PAT = LOG_BASE + '.%02d'
        n = 0
        try:
            while os.path.exists(LOG_PAT % (n + 1)) and n < 100:
                n = n + 1
            while os.path.exists(LOG_PAT % n) and n > 0:
                # Shift existing logs up
                os.rename(LOG_PAT % n, LOG_PAT % (n + 1))
                n = n - 1
            if os.path.exists(LOG_BASE):
                os.rename(LOG_BASE, LOG_PAT % 1)
            self.log = open(LOG_BASE, 'w')
        except Exception as e:
            self.hprint(-1, "Unable to open log file: [%s]" % str(e),
                        file=sys.stderr)
            self.log = open('/dev/null', 'w')
        return True

    @c_logger
    def _run_command(self, command, stage):
        """
        Main execution process, expected to be called from a background thread
        as it blocks while running. Generates a sub-thread itself to enable
        wait()-ing with a timeout.
        """
        if self.job_dir is None:
            raise NameError("Job directory not set!")

        def my_message(s, pre=""):
            return "%s%s: [%s:%s] in [%s]: %s" % (pre,
                                                  stamp(),
                                                  self.NAME,
                                                  stage,
                                                  self.job_dir,
                                                  s)

        if self._prep_log():
            # New log file
            self.log.write(my_message("Processing.", pre="# ") + '\n')

        # Always check files
        self._check_files()

        t_status = {'cond': threading.Condition(),
                    'pid': None,
                    'return': None}
        try:
            def pre_func():
                # Make sure system is flushed before running
                os.setpgrp()
                try:
                    os.nice(self.PRIORITY)
                except OSError:
                    pass

            def task():
                # Used to run subprocess in a separate thread we can
                # wait on with a timeout.
                j = subprocess.Popen(args=shlex.split(command),
                                     close_fds=True,
                                     stdin=open('/dev/null', 'r'),
                                     stdout=self.log,
                                     stderr=subprocess.STDOUT,
                                     cwd=self.job_dir,
                                     preexec_fn=pre_func)
                with t_status['cond']:
                    # Interlocked update of pid or failure (won't set.)
                    # We only wait 5 seconds here; don't bother handling errors
                    # separately; we'll just time out in the creating thread.
                    t_status['pid'] = j.pid
                    t_status['cond'].notify()
                ret = j.wait()
                t_status['return'] = ret

            # Make sure not to sneak in or leave another task runnning.
            with ACTIVE_LOCK:
                if self.PRIORITY > run_level():
                    self.hprint(0, "Waiting our turn.")
                    self.timer.pause()
                    while self.PRIORITY > run_level():
                        WAITING[self.PRIORITY] = True
                        ACTIVE_LOCK.wait()
                    WAITING[self.PRIORITY] = None
                    self.hprint(0, "Able to run.")
                    self.timer.resume()

                # We are holding ACTIVE_LOCK and it is our turn.
                # Make sure anyone else is
                freeze_tasks(self.PRIORITY)

                if len(self.files_found) and '{' in command:
                    # Allow replacements with found files list
                    try:
                        command = command.format(*self.files_found)
                    except (ValueError, IndexError, TypeError) as e:
                        JobDescription.hprint(-1,
                                              "Unable to format command?!")

                self.log.write(my_message('Running command: %s' %
                                          repr(shlex.split(command)),
                                          pre="# ") + '\n')
                self.log.flush()

                j = threading.Thread(target=task)
                with t_status['cond']:
                    j.start()
                    # Should not take long to spawn a thread and Popen.
                    t_status['cond'].wait(5)

                if t_status['pid'] is None:
                    tprint(-1, t_status)
                    raise RuntimeError(my_message("Error starting process!"))

                add_pid(t_status['pid'],
                        self.PRIORITY,
                        self.timer,
                        self.NAME)

            # Max execution time; pauses not charged
            remain = self.TIMEOUT - self.timer.seconds()
            while j.is_alive() and remain > 0:
                j.join(remain)
                remain = self.TIMEOUT - self.timer.seconds()

            if j.is_alive():
                # Timed out.
                kill_pid_tree(t_status['pid'])
                j.join()
                raise RuntimeError(my_message('Killed for timeout!'))

            j.join()  # Joining background thread.

            if t_status['return']:
                raise RuntimeError(my_message('Non-zero exit code: %d' %
                                              t_status['return']))
        except Exception as e:
            # Record exception in recon.log
            self.log.write('# ' + str(e) + '\n')
            self.log.flush()
            raise
        finally:
            # Thread is done at this state; use without lock
            if t_status['pid'] is not None:
                discard_pid(t_status['pid'], self.PRIORITY)
