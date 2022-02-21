====================
How to use Lifeblood
====================

Main components
===============

Lifeblood is a task processing system. It consists of 3 main component types:
* scheduler
* worker
* viewer

scheduler
---------
Scheduler is the main brain of Lifeblood.
It processes tasks, moves them around and schedules them for execution on workers.

worker
------
Worker is the one component that does the (usually) heavy work specified by tasks' invocations.

viewer
------
Viewer is the tool that allows a user to view scheduler's state.
With a viewer you can see the state of the processing of your task group, or modify or create new node setups,
move tasks around, or otherwise change things in scheduler.

-----

All of the components above may run on the same machine, or on different machines within the same local network (for now)
