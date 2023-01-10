# NOTE: this file is a simplified standalone version of lifeblood.environment_resolver
# And therefore this file should be kept up to date with the original
#
# note as well: this is still kept py2-3 compatible

import json


class EnvironmentResolverArguments:
    """
    this class objects specity requirements a task/invocation have for int's worker environment wrapper.
    """
    def __init__(self, resolver_name=None, arguments=None):
        """

        :param resolver_name: if None - treat as no arguments at all
        :param arguments:
        """
        if arguments is None:
            arguments = {}
        if resolver_name is None and len(arguments) > 0:
            raise ValueError('if name is None - no arguments are allowed')
        self.__resolver_name = resolver_name
        self.__args = arguments

    def name(self):
        return self.__resolver_name

    def arguments(self):
        return self.__args

    def serialize(self):  # type: () -> bytes
        return json.dumps(self.__dict__).encode('utf-8')

    @classmethod
    def deserialize(cls, data):  # type: (bytes) -> EnvironmentResolverArguments
        wrp = EnvironmentResolverArguments(None)
        wrp.__dict__.update(json.loads(data.decode('utf-8')))
        return wrp
