.. _nodes/stock/houdini/karma:

=====
Karma
=====

Renders given USD file with karma renderer (part of houdini)

Parameters
==========

:Usd file path:
    path to the usd file to be rendered
:Output image file path:
    path where to save beauty pass
:Skip if result already exists:
    if set - if ``output image file path`` file already exists - render will be skipped

Attributes Set
==============

When render is done, the following attributes are set:

:file:
    set to path of the rendered beauty pass image

Devices
=======

This node understands the following device types:

:gpu:

    Treated as GPU Optix device

    tags used:

    - ``karma_dev`` - has format ``<karma optix device num>/<karma optix device count>``

      - ``<karma optix device num>`` is optix device index, corresponds to whatever karma understands in ``KARMA_XPU_DISABLE_DEVICE_N`` environment variables
        (where N is the optix device index)
      - ``<karma optix device count>`` is the total number of optix devices (which should be the same as the index of the first non-optix/embree device)

      Example values: ``0/2``, ``1/2``, ``0/1``
