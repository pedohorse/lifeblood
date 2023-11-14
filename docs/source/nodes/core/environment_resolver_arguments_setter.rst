.. _nodes/core/environment_resolver_arguments_setter:

=====================================
Modify Environment Resolver Arguments
=====================================

Modifies Environment Resolver information of the task

Parameters
==========

Mode
    * Set - sets/resets environment resolver information
    * Modify - modifies existing environment resolver information
Resolver
    Environment Resolver type name to set
Arguments To Set
    Environment Resolver arguments to set.

    For default Lifeblood's environment resolvers, here you most likely want to set such arguments as
    package names in form of ``package.<package_name>`` to value that can be iterpreted as version specification by Semantic Versioning standard,
    for example ``~=1.2.3``
Arguments To Delete
    In Modify mode - list of environment resolver arguments to delete

Attributes Set
==============

None
