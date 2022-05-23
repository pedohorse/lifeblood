Logging
=======

| All Lifeblood components log to stdout, as well as to a log file.
| Log files location is:

* ``/home/<username>/.local/share/lifeblood/log`` (or ``$XDG_DATA_HOME`` if defined) (linux)
* ``C:\Users\<username>\AppData\Local\lifeblood\log`` (based on ``%LOCALAPPDATA%``) (windows)
* ``/Users/<username>/Library/Application Support/lifeblood/log`` (mac)

By default all components of Lifeblood are set to log only "important" messages (like warnings and errors),
but to figure out what's happening sometimes you would need a bit more information.

| All of Lifeblood's components can accept ``--loglevel`` parameter before command to specify minimum log level to, well, log.
| for example ``lifeblood scheduler --loglevel DEBUG`` will set default log level to DEBUG and you will see all the
  crap printed out into stdout
