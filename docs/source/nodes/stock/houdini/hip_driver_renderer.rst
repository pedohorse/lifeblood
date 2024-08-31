.. _nodes/stock/houdini/hip_driver_renderer:

===================
Hip Driver Renderer
===================

Given a hip file and path to a ROP node inside it - this node will render that ROP node for the frame range defined by ``frames`` attribute.

For each frame it will produce a child task if second input is connected at the time of the processing.

Parameters
==========

:Hip Path:
    Path to the hip file to render
:Mask As Different Hip:
    It is possible to open one hip, but make it look for all expressions inside like it's a different hip file.

    This can be useful if actual hip file is relocated, but it's contents expects it to be in a different place,
    for example when a hip file is copied when submitted to a farm manager.
:mask hip path:
    Hip file path to mask as. This path does not have to exist
:Rop Node Path:
    Path to the ROP(driver) node to render inside the hip file
:ignore rop inputs:
    If True - inputs plugged into the givern Rop Node will be ignored. This is usually the desirable behaviour
:Set These Attribs As Context:
    Task attributes that match this mask will be set as Context Attributes in houdini session
:Render Whole Range (needed for alembics):
    Instead of rendering frame by frame, the whole frame range will be rendered with a single .render() call.
    Alembics require this to be set.
:Skip If Result Already Exists:
    Skip rendering if expected output already exists
:Generate Children For Skipped:
    If set, children tasks will still be generated for skipped frames. This is usually the desired behaviour
:Override Output Path:
    If set - allows overriding output path for the rop node.
:Use Specific Parameter Name For Output:
    If set - allows overriding the name of the parameter on houdini node that contains output path.
:Attributes To Copy To Children:
    Task attributes that match this mask will be copied to children tasks
:Detail Attributes To Extract:
    Geometry detail attributes that match this mask will be copied back to the Lifeblood task
:Detail Intrinsics To Extract:
    Geometry detail intrinsic attributes that match this mask will be copied back to the Lifeblood task

Attributes Set
==============

These attributes are set on **children** tasks

:hipfile:
    Is set to the hip file path that was processed
:file:
    Is set to Rop Node's output path for the generated file

Devices
=======

This node understands the following device types:

.. include:: ./common_devices.rst
