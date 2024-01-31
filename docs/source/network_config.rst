.. _network_config:

=====================
Network Configuration
=====================

.. contents::
    :local:

Overview
========

Lifeblood's components use TCP network connections to communicate. Currently it is limited to ip v4 as this is the most common scenario for
home/office LAN networks.

Additionally UDP is used for non-essential (but enabled by default) broadcasting feature.

This article will help you to open minimal number of ports to allow Lifeblood components to communicate.

Communication Overview
======================

Scheduler, Workers and Worker Pools
-----------------------------------

The main components of Lifeblood, such as Scheduler and Workers communicate to each other using messaging.

Messages can be routed, this way the standard ``Simple Worker Pool`` by default acts as a message proxy for local workers that it spawns.

Scheduler has 3 main ports:

- ``server_port`` (default: 1384)

  Simplified controls, usually used by submitters to reach scheduler
- ``server_message_port`` (default: 1386)

  All message communications to scheduler happen through it.
- ``ui_port`` (default: 1385)

  Viewer connects to scheduler over this port

Message proxies (including ``Simple Worker Pool``) use this port:

- ``message_proxy_starting_port`` (default: 22182)

If multiple message proxies are started - they will try to use first available port starting at the value above.

Therefore it is better to open a couple of consecutive ports starting at 22182, just in case, unless you
know exactly the configuration you are going to be running.

Simple Worker Pool communicates with the workers it spawns locally, each worker's control port is

- ``worker_starting_port`` (default: 6969)

  each new worker will attempt to use first available port starting at the given value

Broadcasting
------------

Broadcasting (enabled by default) is a method for Lifeblood's components to automatically find
the Scheduler on the local network.

Scheduler sends out a message over **UDP** port to all machines on the local network
(if the network is configured to support broadcasts/multicasts). Default broadcasting interval is 10 seconds.

- ``broadcast_port`` (default: 34305)

Example configuration
=====================

Assuming all ports are at default values (they are configurable through config files),
the following configuration may be applied for a tightly configured firewall:

We assume that firewall allows all local communications, therefore we do not have to think about
Simple Worker Pool talking to Workers it spawns.

Machine with Scheduler
----------------------

Opened Incoming Ports: ``1384 1385 1386``

Machine with Simple Worker Pool
-------------------------------

Opened Incoming Ports: from ``22182`` to ``22185``

If same machine runs both Scheduler and Simple Worker Pool - just combine the two sets of ports

Machine with Viewer
-------------------

No incoming connections are happening for the viewer (except possible broadcast)

Broadcasting
------------

If you use the Broadcasting feature to make components recognize each other - you need
to open additional **UDP** port ``34305`` on **each** machine that needs to receive the broadcast
from the scheduler (so all worker machines, and all viewer machines, unless you configure some of them
to not rely on broadcasts)
