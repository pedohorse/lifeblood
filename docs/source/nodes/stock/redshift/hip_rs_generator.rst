.. _nodes/stock/redshift/hip_rs_generator:

====================
Houdini Redshift ROP
====================

Produces Redshift Scene Decscription files (.rs files) from given redshift ROP node.

These .rs files can be rendered with :ref:`redshift node<nodes/stock/redshift/redshift>`

Parameters
==========

.. include:: nodes/standard_resource_requirements.rst

.. include:: nodes/standard_hip_file_masking.rst

:Hip File Path:
    Houdini ``.hip`` file to render rop from
:Rop Node Path:
    Node path in the hip file to the Redshift ROP node to render
:Set These Attribs As Context:
    Attributes that match this mask will be set as context attributes in houdini session (will be accessible with ``@attrib_name`` syntax)
:Scene Description File Path:
    Where to save Redshift Scene file (.rs file)
:Skip If Result Already Exists:
    If file at ``Scene Description File Path`` already exists - skip the work
:Generate Children For Skipped:
    If the work is skipped - this checkbox determines if child task should still be generated or not
:Attributes To Copy To Children:
    Attributes on the main task that match this pattern will be copied to children.

Attributes Set
==============

Main Task
---------

None

Spawned Children Tasks
----------------------

:file:
    Set to the path to .rs scene produced
:frames:
    List of frames, sublist of parent frames list, (most probably a single frame) that this child task represents

Additionally - any attributes copied from parent
