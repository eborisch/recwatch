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

import re
import os
import traceback
import sys

from collections import defaultdict
from textwrap import dedent

# To make these available to configurations
from time import time, sleep

from reclib.util import dprint
from reclib.job_description import JobDescription

_PREFIX = os.environ['HOME'] + '/incoming'

def set_prefix(newval=None):
    global _PREFIX
    if newval is not None:
        _PREFIX = newval

def get_prefix():
    return _PREFIX

def load_conf(conf_dir, enabled, disabled):
    """
    All tasks in a given directory share an item in loaded; one 'fd' is opened
    for monitoring; individual tasks go into 'tasks' dict; directory mod time
    goes into mtime.
    Returns loaded.
    """
    loaded = defaultdict(dict)

    if enabled is not None:
        enabled = '(' + '|'.join(enabled) + ')'

    if disabled is not None:
        disabled = '(' + '|'.join(disabled) + ')'

    class JobError(Exception):
        err_format = dedent("""\
            #!  Error while parsing configuration file [{}]:
            #!  {}
            #!  Skipping.""")

        def __init__(self, conf, msg):
            self.conf = conf
            self.msg = msg

        def __str__(self):
            return JobError.err_format.format(self.conf, self.msg)

    # Load configurations and open directories for watching
    conf_list = []
    for root, dirs, files in os.walk(conf_dir):
        for f in files:
            if not re.match(r".*\.conf$", f):
                dprint(0, "#  Skiping %s." % f)
                continue

            if enabled and not re.search(enabled, f):
                dprint(0, "#  Skiping %s. (Did not match --enable)" % f)
                continue

            if disabled and re.search(disabled, f):
                dprint(0, "#  Skiping %s. (Matched --disable)" % f)
                continue
            conf_list.append(os.path.join(root, f))

    for f in sorted(conf_list):
        dprint(0, "#  Parsing %s:" % f)

        # JOBS dictionary from conf: [(dir,key)] = JobDescription
        g = globals().copy()
        g['JOBS'] = {}
        try:
            execfile(f, g)
        except Exception as e:
            print("#   Unable to parse job description in [%s]." % f)
            print("#   Error: [%s]" % str(e))
            for x in traceback.format_exception(*sys.exc_info()):
                print(x)
            continue

        if 'JOBS' not in g or type(g['JOBS']) is not dict:
            print("#   JOBS dictionary not defined while parsing. Skipping.")
            continue

        def JErr(x):
            "Helper function to throw job errors."
            raise JobError(os.path.basename(f), x)

        for i, job in g['JOBS'].iteritems():
            # Argument checking from parsed file.
            try:
                if type(i) is not tuple or len(i) is not 2:
                    JErr("Keys for JOBS must be a (path, pattern) tuple; "
                         "found >>{}<< instead".format(i))

                for n in i:
                    if type(n) is not str:
                        JErr("Keys for JOBS must be a (path, pattern) tuple; "
                             "found >>{}<< instead.".format(i))

                if type(job) is not JobDescription:
                    JErr("Items in JOBS must be a JobDescription object;"
                         " found JOBS[{}]=>>{}<< instead.".format(i, job))

                d = os.path.abspath(os.path.join(_PREFIX, i[0]))
                k = i[1]

                if not os.path.exists(d):
                    JErr("Requested watch path [{}] does not exist.".format(d))

                v = loaded[d]
                if len(v) == 0:
                    v['tasks'] = {}
                    try:
                        # Open and save the FD for this directory
                        v['fd'] = os.open(d, os.O_RDONLY)
                    except OSError as e:
                        # If we are here, we must have just added d to v.
                        loaded.pop(d)
                        JErr("Error opening directory [{}] "
                             "for reading:{}".format(d, str(e)))

                v['tasks'][k] = job
                # Initialize (create) queues by trying to access them
                # as queues is a defaultdict(Queue.Queue)
                JobDescription.queues[job.PRIORITY]
                v['mtime'] = 0  # Will check at start for stale files
                dprint(0,
                       "#    Job [%s (%d)] watching in [%s] for [%s]" %
                       (job.NAME, job.PRIORITY, d, k))
            except JobError as e:
                print(str(e))
                continue
    return loaded


