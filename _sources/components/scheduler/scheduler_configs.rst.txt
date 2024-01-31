.. _scheduler configs:

================
Scheduler Config
================

All scheduler-related configuration files are located in :ref:`config-dir` ``/scheduler``

All configuration files we are going to mention below are located there (unles explicitly
stated otherwise)

Main Configuration file
=======================

The main configuration file used by Scheduler is config.toml

As it implies, configuration uses `toml language <https://toml.io/>`_, which
should be a bit more user-friendly than yaml.

When you first start the Scheduler - it will generate a default configuration file. That file
contains some general sections with commented example settings that could be changed

This file covers things like what network interfaces/ports to use, default database file and some of it's configuration

the default config will look something like this:

::

    [core]
        ## you can uncomment stuff below to specify some static values
        ##
        # server_ip = "192.168.0.2"
        # server_port =1384
        # ui_ip = "192.168.0.2"
        # ui_port = 1385

        ## you can turn off scheduler broadcasting if you want to manually configure viewer and workers to connect
        ## to a specific address
        # broadcast = false

        [scheduler]

        [scheduler.database]
        ## you can specify default database path,
        ##  but this can be overriden with command line argument --db-path
        # path = "/path/to/database.db"

        ## uncomment line below to store task logs outside of the database
        ##  it works in a way that all NEW logs will be saved according to settings below
        ##  existing logs will be kept where they are
        ##  external logs will ALWAYS be looked for in location specified by store_logs_externally_location
        ##  so if you have ANY logs saved externally - you must keep store_logs_externally_location defined in the config,
        ##    or those logs will be inaccessible
        ##  but you can safely move logs and change location in config accordingly, but be sure scheduler is not accessing them at that time
        # store_logs_externally = true
        # store_logs_externally_location = /path/to/dir/where/to/store/logs

nodes
=====

This folder contains node type specific configurations

.. _custom node type configuration:

custom node type configuration
------------------------------

File ``nodes/config.toml`` is a `toml <https://toml.io/>`_ config file that can contain any arbitrary values
belonging to any node type.

The format of this config is the following: every section name represents a node type name, and every key
within that section - is a gettable key from `config` object in parameter expressions (see :ref:`parameter expressions`)

for example:

::

  [matrixnotifier]
  token = 'longstrongtoken'
  room = '!roomname:myserver.org'

or (which is the equivalent accordint to toml)

::

  matrixnotifier.token = 'longstrongtoken'
  matrixnotifier.room = '!roomname:myserver.org'

now only in a node of type "matrixnotifier" a parameter expression can use ``config['room']``
to get the value  ``'!roomname:myserver.org'``

This is very convenient in combination with `custom default parameter values`_

Custom default parameter values
-------------------------------

File ``nodes/defaults.toml`` is a `toml <https://toml.io/>`_ config file that can contain default ref:`node settings` names

This file contains simple key-value pairs, where key is a node type name, and value is the name of a named ref:`node settings`
for this node type.

The specified node settings will be applied on node creation (on scheduler's side, not on viewer's side)
