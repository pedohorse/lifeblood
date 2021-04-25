"""
environment wrapper is an object that produces runtime environment for an invocation
taking into account invocation's requirements and worker's machine specifics

For example you might want to at least pass the version of software you want to use together with
your invocation job

Or a more complicated environment wrapper would take in a whole set of required packages from invocation job
and produce an environment fitting those requirements

As I see the it, a freelancer or a studio would implement one specific to them environment wrapper
for all workers, not several different wrappers
"""
import os
from .invocationjob import Environment, InvocationEnvironment


class BaseEnvironmentWrapper:
    def get_environment(self, requirements: dict, additional_env: InvocationEnvironment):
        """
        this is the main reason for environment wrapper's existance.
        give it your specific requirements
        :param additional_env:
        :param requirements:
        :return:
        """
        raise NotImplementedError()


class TrivialEnvironmentWrapper(BaseEnvironmentWrapper):
    """
    trivial environment wrapper does nothing
    """
    def get_environment(self, requirements: dict, additional_env: InvocationEnvironment):
        env = Environment(os.environ)
        return additional_env.resolve(base_env=env)
