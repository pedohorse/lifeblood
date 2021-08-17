[![Tests](https://github.com/pedohorse/taskflow/actions/workflows/python-app.yml/badge.svg?branch=dev)](https://github.com/pedohorse/taskflow/actions/workflows/python-app.yml)

## Task Flow
is a task/job management package  
in terms of CG industry it's a mini render farm manager

it consists of **scheduler**, **workers** and **GUIs**

a scheduler manages multiple workers, giving them tasks to complete.  
GUI tool is used to check on the state of scheduler and tasks.

This system overview should be very familiar to anyone who saw at least one renderfarm.

### Original Purpose
the purpose of this particular task manager is to be:
- instantly and easily deployable
- aim for smaller farms or even individual setups
- keep potential scaling in mind
- be integrateable (to some extent) with existing farms
- replacement for TOPs for any non-houdini related tasks

## UNDER DEVELOPMENT
[task tracking in trello](https://trello.com/b/sSbc8u6M/taskflow)
