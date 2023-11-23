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
