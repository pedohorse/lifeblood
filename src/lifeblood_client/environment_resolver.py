# NOTE: this file is a simplified standalone version of lifeblood.environment_resolver
# And therefore this file should be kept up to date with the original
#
# note as well: this is still kept py2-3 compatible

import json


class EnvironmentResolverArguments:
    """
    this class objects specify requirements a task/invocation have for it's worker environment wrapper.
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
        return json.dumps({
            '_EnvironmentResolverArguments__resolver_name': self.__resolver_name,
            '_EnvironmentResolverArguments__args': self.__args,
        }).encode('utf-8')

    @classmethod
    def deserialize(cls, data):  # type: (bytes) -> EnvironmentResolverArguments
        wrp = EnvironmentResolverArguments(None)
        data_dict = json.loads(data.decode('utf-8'))
        wrp.__resolver_name = data_dict['_EnvironmentResolverArguments__resolver_name']
        wrp.__args = data_dict['_EnvironmentResolverArguments__args']
        return wrp
