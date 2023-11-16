.. _nodes/core/spawn_children:

==============
Spawn Children
==============

Spawn children for incoming task.

Tasks' :ref:`parent-children hierarchy<task_parent_hierarchy>` is a core concept of Lifeblood.

Children will be going out from the second output.

Parameters
==========

Spawn This Many Children
    Number of children to spawn
Child Number Attribute To Create
    Create attribute with this name, containing the number of child created by this node
Children Base Name
    Base name for children tasks
Attribute Pattern To Inherit
    pattern of attributes to inherit by children from the original task


Attributes Set
==============

Incoming task is unmodified.