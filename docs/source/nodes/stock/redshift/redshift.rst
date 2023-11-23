.. _nodes/stock/redshift/redshift:

========
Redshift
========

Renders given .rs file with redshift renderer

Parameters
==========

:rs file path:
    path to the rs file to be rendered
:output image file path:
    path where to save beauty pass. All AOVs if enabled will be saved to the base directory of this file

    .. note::
        Due to redshift implementation specifics, redshift will first render to temporary location,
        and only after that files will be moved to their final destination. So you have to make sure
        you have enough space in the temporary location for some number of frames that are going to render in parallel

:skip if result already exists:
    if set - if ``output image file path`` file already exists - render will be skipped

    .. warning::
        Current limitation: when frames are skipped - ``files`` attribute is not set to AOV paths,
        as AOVs paths are only received from rendering process.

Attributes Set
==============

When render is done, the following attributes are set:

:file:
    set to path of the rendered beauty pass image
:files:
    set to a list of paths to all AOVs produced by the render
