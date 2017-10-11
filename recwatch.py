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

import argparse
import os
import re
import signal
import subprocess
import sys

from collections import defaultdict
from errno import EEXIST
from tempfile import mktemp
from time import sleep

from reclib.util import dprint, atomic_create, tprint, stamp, set_email
from reclib.vars import DEBUG
from reclib.pid import run_level, get_populated
from reclib.jobs import QueueRunner, run_jobs
from reclib.job_description import JobDescription
from reclib.conf import set_prefix, load_conf, get_prefix

OURPATH = os.path.split(os.path.abspath(__file__))
PIDPATH = os.path.join(OURPATH[0], '.' + OURPATH[1] + '.PID')


def highlander(sacrifice=False, wait=0, quiet=False, _retried=False):
    """
    Tests for existence of a conflicting (running) process.

    Keyword arguments:
    sacrifice -- Attempt to kill (SIGTERM) running process.
    wait -- Seconds to wait for running process to disappear.
    quiet -- Exit silently if running process exists.
    _retried -- (internal) Used to prevent infinite loops.
    """
    # There can be only one.
    while os.path.exists(PIDPATH):
        try:
            LF = open(PIDPATH, 'r')
        except IOError as e:
            if e.errno == 2:
                # File disappeared
                break
        PID = int(LF.read())
        LF.close()

        try:
            if os.path.exists('/proc'):
                # This will raise IOError if the the PID is gone
                res = open('/proc/{}/cmdline'.format(PID), 'r').read()
                res = res.replace('\0', ' ')
            else:
                # This will raise CalledProcessError if the the PID is gone
                res = subprocess.check_output(("ps",
                                               "-o", "args=",
                                               "-p", str(PID)),
                                              universal_newlines=True).strip()

            if OURPATH[1] not in res:
                print("# Previous PID is not us; ignoring.\n# " + res)
                raise LookupError

            if quiet:
                # If the above lines did not fail, the PID is still running.
                # Exit gracefully in this condition with --quiet
                sys.exit(0)

            print("# Another watcher is already running?\n#   " + res)
            if sacrifice:
                print("# Sending exit signal.")
                os.kill(PID, signal.SIGTERM)

            if wait > 0:
                sleep(1)
                wait = wait - 1
            else:
                print("# Existing watcher still running. Giving up.")
                sys.exit(1)
        except (IOError, subprocess.CalledProcessError):
            if not sacrifice:
                print("# Stale PID file found. Cleaning up.")
            os.unlink(PIDPATH)
        except LookupError:
            os.unlink(PIDPATH)

    try:
        atomic_create(PIDPATH, "{0}".format(os.getpid()))
    except OSError as e:
        # Hrmm. Did someone sneak in?
        if e.errno == EEXIST and not _retried:
            highlander(sacrifice, wait, quiet, _retried=True)
        else:
            raise


if __name__ == '__main__':
    script_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(OURPATH[0])
    # This makes pausing cluster jobs possible. Small performance hit possible
    # (for mvapich2 clusters; won't hurt anything else.)
    os.environ['MV2_USE_BLOCKING'] = '1'
    JOB_DIRS = defaultdict(dict)

    parse = argparse.ArgumentParser(description="Automatic recon launcher.")
    paa = parse.add_argument
    paa("--checkonly", default=False, action='store_const',
        const=True,
        help='Only check/kill (if --kill) running instance.')
    paa("--conf_dir",
        help="[jobs.conf.d] Location of configuration files.",
        default="jobs.conf.d",
        metavar="<dir>")
    paa("--daemon", default=False, action='store_const',
        const=True,
        help="Drop into background (log into watcher.log)")
    paa("--disable", action='append', default=None,
        help='Configuration files (pattern) to disable.')
    paa("--enable", action='append', default=None,
        help='Configuration files (pattern) to enable.')
    paa("--fake", default=False, action='store_const',
        const=True, help='With --daemon, test daemon code.')
    paa("--kill", default=False, action='store_const',
        const=True,
        help='Attempt to stop already running instance.')
    paa("--email", default=None,
        help="[None] Notification e-mail address.",
        metavar="<email>")
    paa("--prefix_dir", default=get_prefix(),
        help="[{}] Base path for directories".format(get_prefix()),
        metavar="<dir>")
    paa("--quiet", default=False, action='store_const',
        const=True,
        help='Exit quietly if already running; notify if not.')
    paa("--wait", type=int, default=0,
        help='Seconds to wait for previous task to exit.')

    args = parse.parse_args()

    if (args.checkonly or args.kill) and args.wait == 0:
        args.wait = 1

    highlander(sacrifice=args.kill, wait=args.wait, quiet=args.quiet)

    if args.email:
        if re.match('[^ ]+@[^ ]+', args.email) is not None:
            set_email(args.email)

    if args.quiet:
        # We only reach here on --quiet if there was no watcher running
        print("No watchers running; attempting to launch new watcher.")

    if args.checkonly:
        os.unlink(PIDPATH)
        sys.exit(0)

    if args.daemon:
        STDOUT_SAVE = None
        try:
            if not args.fake:
                # Close down stdin/0 completely
                sys.stdin.close()
                os.close(0)
                # Since we just closed 0, this attaches '/dev/null' to fid 0
                sys.stdin = open('/dev/null', 'r')

                # Redirect stdout
                # We use this to write a "launched" message below.
                STDOUT_SAVE = os.dup(1)
                sys.stdout.flush()
                sys.stdout.close()
                os.close(1)
                # Since we just closed 1, this attaches 'watcher.log' to fid 1
                sys.stdout = open('watcher.log', 'a', buffering=0)

                # Redirect stderr
                sys.stderr.flush()
                sys.stderr.close()
                sys.stderr = sys.stdout

                # Duplicate watcher.log's fileno() to fid 2
                os.dup2(sys.stderr.fileno(), 2)
        except Exception as e:
            print("Error redirecting I/O: [{}]".format(str(e)),
                  file=sys.stderr)
            if STDOUT_SAVE is not None:
                os.dup(STDOUT_SAVE, 1)
                os.close(STDOUT_SAVE)
                STDOUT_SAVE = None

        # Go through double fork process "just in case."
        for n in range(2):
            PARENT = os.getpid()
            CHILD = os.fork()

            if CHILD > 0:
                # Parent; update PID in file; file at PIDPATH was written in
                # highlander() call above. By doing this here, the file always
                # contains a running PID.
                TMP_NAME = mktemp(dir=".")
                atomic_create(TMP_NAME, "{0}".format(CHILD))
                os.rename(TMP_NAME, PIDPATH)
                os.close(STDOUT_SAVE)
                sys.exit(0)
            else:
                if n == 0 and not args.fake:  # First time only.
                    os.setsid()
                # Child
                sleep(0.5)
                while open(PIDPATH, 'r').read().strip() != str(os.getpid()):
                    print("# Waiting... {}".format(PARENT))
                    sleep(1)
                    subprocess.call("sync")

    # This is only used for catching a quit request (signal)
    RUNNING = True

    def exit_handler(num, fr):
        global RUNNING
        RUNNING = False

    signal.signal(signal.SIGTERM, exit_handler)

    set_prefix(args.prefix_dir)
    conf_dir = os.path.join(script_dir, args.conf_dir)
    # Load configurations and open directories for watching
    dprint(0, "# Parsing configuration files:")
    JOB_DIRS = load_conf(conf_dir, enabled=args.enable, disabled=args.disable)

    START_MSG = "%s: Configuration parsed. " \
                "Starting execution. [%d]" % (stamp(), os.getpid())
    dprint(0, START_MSG)

    if args.daemon and STDOUT_SAVE is not None:
        os.write(STDOUT_SAVE, START_MSG)
        os.close(STDOUT_SAVE)
        STDOUT_SAVE = None

    # Start background threads; one per queue (one per priority level used)
    runners = []
    for q in JobDescription.queues.itervalues():
        runners.append(QueueRunner(q))
        runners[-1].start()

    while RUNNING:
        try:
            for d, n in JOB_DIRS.iteritems():
                # We already have a descriptor open to this directory. Just
                # fstat the descriptor to check for modification.
                d_m = os.fstat(n['fd']).st_mtime
                if d_m == n['mtime']:
                    continue
                # Update stored modification time
                n['mtime'] = d_m
                run_jobs(d, n['tasks'])
            sleep(1)
            if DEBUG >= 3:
                tprint(3, "Runlevel: %d" % run_level())
                tprint(3, "ACTIVE: " + repr(get_populated('ACTIVE')))
                tprint(3, "PAUSED: " + repr(get_populated('PAUSED')))
                tprint(3, "WAITING: " + repr(get_populated('WAITING')))
        except KeyboardInterrupt:
            # Don't cancel background jobs on multiple SIGINTs
            signal.signal(signal.SIGINT, signal.SIG_IGN)
            # Get to new line (after ^C is printed by shell)
            dprint(0, "")
            tprint(0, "Exiting due to interrupt. Waiting for queued jobs.")
            break
        except Exception as e:
            # We just keep going; never exit watcher process just because a
            # job failed.
            tprint(-1,
                   "Error during processing: [%s]" % str(e),
                   file=sys.stderr)

    if not RUNNING:
        tprint(0, "Exiting due to signal. Waiting for queued jobs.")

    tprint(1, 'Queue end-of-execution tasks.')
    for q in JobDescription.queues.itervalues():
        q.put(None)

    tprint(1, 'Waiting for work queues to drain.')
    for q in JobDescription.queues.itervalues():
        q.join()

    tprint(1, 'Waiting for threads to join.')
    for r in runners:
        r.join()

    dprint(1, repr(dir()))
    dprint(1, repr(JOB_DIRS))
    for j in JOB_DIRS:
        for t in JOB_DIRS[j]['tasks'].itervalues():
            if t.completed:
                t.hprint(0, "Exiting -- processed %d." % t.completed)
    os.unlink(PIDPATH)
    sys.exit(0)

# vim: set sw=4 ts=4 et
