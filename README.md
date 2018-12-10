# recwatch
Service to watch for and process remotely submitted tasks.

```
usage: recwatch.py [-h] [--checkonly] [--conf_dir <dir>] [--daemon]
                   [--disable DISABLE] [--enable ENABLE] [--fake] [--kill]
                   [--email <email>] [--prefix_dir <dir>] [--quiet]
                   [--wait WAIT]

Automatic recon launcher.

optional arguments:
  -h, --help          show this help message and exit
  --checkonly         Only check/kill (if --kill) running instance.
  --conf_dir <dir>    [jobs.conf.d] Location of configuration files.
  --daemon            Drop into background (log into watcher.log)
  --disable DISABLE   Configuration files (pattern) to disable.
  --enable ENABLE     Configuration files (pattern) to enable.
  --fake              With --daemon, test daemon code.
  --kill              Attempt to stop already running instance.
  --email <email>     [None] Notification e-mail address.
  --prefix_dir <dir>  [$HOME/incoming] Base path for directories
  --quiet             Exit quietly if already running; notify if not.
  --wait WAIT         Seconds to wait for previous task to exit.
```
