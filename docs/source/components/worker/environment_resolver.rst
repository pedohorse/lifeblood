.. _environment_resolver:

====================
Environment Resolver
====================

What is it
==========

When you create a task that requires calculations being done in other softwares, like, for example, render in houdini,
You generally want to be able to specify the versions of the software and it's plugins, and other related configurations.

In CG industry (and not only) that is commonly done with environment resolvers.

You provide a resolver with requirements, and it generates an environment for you, or straight away runs
whatever you requested in an environment that satisfies the requirements.

A commonly used resolver in CG is ``rez``

In Lifeblood
============

Lifeblood has a concept of environment resolvers, existing ones can be wrapped as plugins for lifeblood (NOT YET STREAMLINED!!)

.. _standard_environment_resolver:

Standard Environment Resolver
-----------------------------

By default, lifeblood comes with it's own simplest implementation of an environment resolver.
You provide it with requirements that is pairs of "package name" and "package version".

"Package name" can be anything, "package version" is a semantic version. This pair matches exactly one
entry from standard environment resolver config

The configuration file for standard environment resolver is located at :ref:`config-dir` ``/standard_environment_resolver/config.toml``

You can add any package to there in the following form:

.. code-block:: toml

    [packages."package_name"."1.2.3"]
    label = "this is a cool package package_name"
    env.PATH.append = "/path/to/some/bin"
    env.SOMETHING = "a value for env variable SOMETHING"
    env.HOUDINI_PATH.prepend = [
      "/path/related/things/can/also/take/a/list"
      "/of/strings"
      "/whatever/you/want/to/prepend"
    ]

In the example above we have defined package ``package_name`` with version ``1.2.3``

when this package is requested for a task's invocation - the invocation will be started
in an environment, where:

- environment variable ``SOMETHING`` is set to ``"a value for env variable SOMETHING"``,
- ``"/path/to/some/bin"`` is append to system value of ``PATH``
- 3 extra paths are prepended to existing value of ``HOUDINI_PATH`` environment variable

.. note::
    in this example we don't care about correctness of ``HOUDINI_PATH`` environment variable,
    as a correct value would either have ``&`` as final path, or has explicit paths to HFS added.
    So here - it's just a demonstration, not a real configuration example

A real example of houdini configuration would look something like this:

.. code-block:: toml

    [packages."houdini.py3_10"."20.0.506"]
    label = "SideFX Houdini 20.0.506"
    env.PATH.prepend = "/opt/hfs20.0.506/bin"

For example, Houdini's Lifeblood submitter by default creates a requirement for a
Standard Environment Resolver with required package set to ``houdini.py<py_version>``
of version equals to the version of the running houdini. where ``<py_version>``
is the version of python that houdini uses in form of two primary version components,
separated by underscore ``_``, for ex ``3_9 3_10 3_11``

The version is matched against loaded package following `semantic version standard <https://semver.org/>`_,
so requirement of ``houdini 20.0`` will match the package with highest build number.
By default houdini submitter sets requirements to ``~=major.minor.build``, this will match
packages with same ``major`` and ``minor`` versions, and any ``build`` higher or equals to given.

Auto Detection
**************

Standard Environment Resolver when generating initial config does try to scan some common places
to detect certain DCCs installations.

You can check what it detects with

  ``lifeblood resolver scan``

This command will just print out generated config, no files will be written.

To write auto-detected configuration to the standard lifeblood's :ref:`config-dir` - run

  ``lifeblood resolver generate``
