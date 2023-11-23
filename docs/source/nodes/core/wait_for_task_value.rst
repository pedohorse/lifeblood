.. _nodes/core/wait_for_task_value:

===================
Wait For Task Value
===================

The node keeps a GLOBAL pool of values.

When a task is processed:

- task's "condition value" is added to the GLOBAL value pool
- task's "Expected Value(s)" are tested against the node's value pool
    - if **ALL** expected values are found in the pool - task is released
    - else - task will be waiting

All inputs are the same, they are there for convenience, to separate multiple streams.
Tasks from input number N will exit through corresponding output number N.

.. note::
    changing "Condition Value" will **NOT** take into account the tasks that were **ALREADY** processed,
    so be very careful changing this on a live graph

.. note::
    GLOBAL value pool means that all tasks from all task groups contribute to the pool
    so if you need to have pools per group (in case of non-intersecting groups for ex) -
    you will have to ensure values are unique per group, for example by prepending group name
    but it all depends on specific case.

Parameters
==========

:Condition Value:
    Value to be contributed to the "Global Pool"
:Expected Value(s):
    Values that task expects in order to proceed.

Attributes Set
==============

None