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
import threading
import socket

from fnmatch import filter as fnfilter
from time import sleep, time

from reclib.util import dprint, tprint, mail_log

HOSTNAME = socket.getfqdn()

class QueueRunner(threading.Thread):
    """
    Spawns a thread that listens to the specified queue and executes (in
    mode within the thread) jobs in order.
    """
    def __init__(self, queue):
        threading.Thread.__init__(self)
        self.queue = queue

    def run(self):
        dprint(1, "Thread entered")
        while True:
            j = self.queue.get(block=True, timeout=None)
            self.queue.task_done()

            if j is None:
                # This is used to indicate end of work (exiting)
                dprint(1, "Thread exiting")
                return
            (job, job_dir, jTimer) = j
            msg = "Running in [%s]" % job_dir
            if jTimer.seconds() > 1.0:
                msg = msg + "; DELAYED by [%0.1fs]." % jTimer.seconds()
            job.hprint(0, msg)

            mail_log("%s: running on %s" % (job.NAME,
                                            HOSTNAME),
                     "%s running on %s in %s." % (job.header(),
                                                  HOSTNAME,
                                                  job_dir))
            # Actual execution
            try:
                # All of the actual processing is done here.
                t = job.process_dir(job_dir)
                msg = "Completed [%s] in %0.1fs" % (job_dir, t)
                if jTimer.seconds() > t + 1.0:
                    msg = msg + " [%0.1fs]" % jTimer.seconds()
                job.hprint(0, msg)
            except Exception as e:
                mail_log("Error running %s on %s!" % (job.NAME, HOSTNAME),
                         "%s errored on %s in [%s]:\n%s" % (job.header(),
                                                            HOSTNAME,
                                                            job_dir,
                                                            str(e)))


def run_jobs(directory, job_list):
    """
    Designed to be called by main thread monitoring loop when a directory
    being watched is updated. Runs through the list job_list.keys() to see
    if a file matching one of the keys exists. If it does, read in the
    contained directory name[s] and enque tasks. Remove the read keyfile.
    Trailing '/'s in directory names are removed.

    This will get called a second time to make sure no additional
    key files have snuck in.
    """

    dprint(1, "run_jobs(%s, %s)\n" % (directory, repr(job_list)))
    names = os.listdir(directory)
    for k in job_list:
        for n in fnfilter(names, k):
            dprint(1, "Matched: [%s] [%s]\n" % (k, n))
            fpath = os.path.join(directory, n)
            # Supports multiple jobs in one file
            # Wait for file to settle
            mtime = os.stat(fpath).st_mtime
            while mtime + 1 > time():
                dprint(2, "Waiting for keyfile to settle.")
                mtime = os.stat(fpath).st_mtime
                sleep(1)
            with open(fpath) as keyfile:
                # Remove from directory right away
                os.unlink(fpath)
                for jdir in keyfile:
                    jdir = jdir.strip()
                    dprint(2, "Found jobdir [%s] in [%s]" % (jdir, n))
                    if len(jdir) == 0:
                        tprint(0, "No path defined in [%s]" % n)
                        continue
                    # Normalize (remove any '/'s from end)
                    while jdir[-1] == '/':
                        jdir = jdir[:-1]
                    jpath = os.path.join(directory, jdir)
                    if not os.access(jpath, os.R_OK | os.W_OK | os.X_OK):
                        tprint(0, "Unable to enter [%s]" % jpath)
                        continue
                    job_list[k].enqueue(jpath)


