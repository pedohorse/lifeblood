.. _worker:

================
Lifeblood Worker
================

.. contents::
    :local:

Overview
--------

Worker is the part of Lifeblood that is responsible for launching things as ordered by scheduler. A task may (or may not)
create a payload that needs to be computed. For example: run simulation caching, render a frame, generate procedural
geometry, etc.

These payloads are not run by Scheduler directly, instead the Scheduler assigns this payload to one of free workers.
The worker will do the job and report back to the Scheduler with results (or error)

| You would have a single scheduler and multiple workers ran across local network.
| Worker knows about computational resources available on the machine it runs on, and reports that to scheduler.

Multiple workers may be launched on the same machine, they should be able to agree with each other on how to share
resources among each other

Workers can be started on the same machine with scheduler, BUT you should be careful to leave enough resources for
scheduler to work. If machine runs out of memory - scheduler will have problems working and may crash
or be killed by the system.

Pre-requisites
--------------

currently worker requires:

* existing temporary directory, shared between all worker instances (:ref:`scratch location`)
* all worker instances on the same machine must share the same network namespace

Launching
---------

  ``lifeblood worker``

here you would also supply component-specific command line arguments, for example

  ``lifeblood --loglevel DEBUG worker``

See all options using built-in help:

  ``lifeblood worker --help``

It's important to understand: one worker can launch only one payload process at one time,
there is no concept of slots. Instead, multiple workers running locally negociate with each other to share resources
properly.

So all locally running workers will inform each other about the amount of resources that the task they
currently run requested, sharing all machine resources without overspending.

On a machine with a lot of CPU cores for example it makes sense to have more than one workers running
to use resources efficiently, and it can be tedious to manually launch all workers.

For that reason worker :ref:`pools exist <usage pools>`. Generally it's a bigger concept, in this case we need a simplest worker pool
implementation - simple local worker pool.

This pool operates according to the following logic:

* if there is no free workers and we are not at worker limit - launch a worker
* a worker that finishes a task terminates

well, that's it, not much complicated logic in there, but it's more powerful than a worker with constant number of slots:

* pool launches a worker (let's call it worker1), it advertises to scheduler all of it's (for example) 32 cores and 64 GBs of ram
* scheduler assigns worker1 a task that requires 8 cpu cores adn 16GB ram
* there's no free workers now, so pool launches another worker (worker2). worker2 will communicate with worker1
  and learn about the task it's executing, so worker2 will only advertise 24 cores and 48 GBs of ram to the scheduler
* scheduler assigns worker2 a task that requires 20 cores and 40GB of ram
* there's no free workers now, so pool launches another worker (worker3). worker3 will communicate with worker1
  and worker2 and set it's resources to 4 cores and 8 GBs of ram
* now worker1 finishes task and terminates, worker2 and worker3 are informed, and both of them adjust the resources
  advertised to scheduler to 12 cores and 24 GB of ram, though worker2 is still working on it's task
* worker2 finishes task and terminates, worker3 is informed, so it adjusts resources advertised to scheduler to full
  machine's resources: 32 cores and 64 GBs of ram.

and this can get as granular as resources allow. That's why always make sure to set up non zero resources usage for your tasks
and limit maximum workers that pool can spawn to some sane and safe amount.


.. include:: logging.rst