.. _nodes/stock/houdini/mantra:

======
Mantra
======

Renders given IFD file with mantra renderer (part of houdini)

Parameters
==========

:Ifd file path:
    path to the ifd file to be rendered
:Image path:
    path where to save beauty pass
:Skip if result already exists:
    if set - if ``output image file path`` file already exists - render will be skipped

Attributes Set
==============

When render is done, the following attributes are set:

:file:
    set to path of the rendered beauty pass image
