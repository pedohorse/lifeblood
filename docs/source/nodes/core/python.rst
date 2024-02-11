.. _nodes/core/python:

======
Python
======

Execute some python code

Parameters
==========

:good exit codes:
    What invocation process exit codes to consider as "good". Default: ``0``

    This only affects code in ``Invoke`` parameter

:retry exit codes:
    What invocation process exit codes should signal scheduler to retry the invocation as opposed to setting task into ERROR state.

    This only affects code in ``Invoke`` parameter

:Process:
    Python code to be executed by scheduler as part of processing stage. Do not perform any heavy operations here,
    instead perform them in the **invoke** code.

    You can call special function ``schedule()`` here, in that case code from **Invoke** parameter will be scheduled to be executed on a worker.
:Invoke:
    Python code to be scheduled and executed on a worker.

    Use ``task`` variable to get task attributes here, e.g. ``task['attr'] = 123`` will set attribute ``attr`` on the task to value 123.
    While this code will be executed on remote worker, all calls to task attribute getting methods, like ``task['smth']`` will be resolved beforehand.

Attributes Set
==============

Attributes set by code with something like ``task['attr_name'] = "something"`` will be set to the task
