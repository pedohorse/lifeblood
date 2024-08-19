import os
import shutil
import tempfile
from unittest import TestCase
from enum import Enum
import inspect
from lifeblood.scheduler_config_provider_base import SchedulerConfigProviderBase
from lifeblood.scheduler_config_provider_file import SchedulerConfigProviderFile, SchedulerConfigProviderDefaults
from lifeblood.config import Config


class TestSchedulerConfigProviderFile(TestCase):

    @staticmethod
    def __uncomment_default_lines(text: str) -> str:
        class ParsingState(Enum):
            SCANNING = 0
            UNCOMMENTING = 1

        lines = text.splitlines()
        final_lines = []

        state = ParsingState.SCANNING
        for line in lines:
            if state == ParsingState.SCANNING:
                if line.startswith('# ['):
                    final_lines.append(line[2:])
                    state = ParsingState.UNCOMMENTING
                else:
                    final_lines.append(line)
                    if line.rstrip() in ('## defaults',):
                        state = ParsingState.UNCOMMENTING
            #
            elif state == ParsingState.UNCOMMENTING:
                if line.startswith('# '):
                    final_lines.append(line[2:])
                else:
                    state = ParsingState.SCANNING
                    final_lines.append(line)
            else:
                raise NotImplementedError()

        return '\n'.join(final_lines)

    @classmethod
    def setUpClass(cls):
        cls.config_base_path = tempfile.mkdtemp('test_lifeblood')
        os.makedirs(os.path.join(cls.config_base_path, 'scheduler'), exist_ok=True)

    @classmethod
    def tearDownClass(cls):
        if os.path.exists(cls.config_base_path):
            shutil.rmtree(cls.config_base_path)

    def test_default_config_parsing(self):
        default_config_text = SchedulerConfigProviderFile.generate_default_config_text()

        # uncomment things that should do the same as default config
        default_config_text = self.__uncomment_default_lines(default_config_text)

        config_path = os.path.join(self.config_base_path, 'scheduler', 'config.toml')
        with open(config_path, 'w') as f:
            f.write(default_config_text)

        config = Config(config_path)
        config_provider = SchedulerConfigProviderFile(config, Config())
        config_provider_default = SchedulerConfigProviderFile(Config(), Config())

        for meth in inspect.getmembers(SchedulerConfigProviderBase(), inspect.ismethod):
            if meth[0] in ('node_configuration',):
                continue  # TODO: maybe add to tests too

            self.assertEqual(
                getattr(config_provider_default, meth[0])(),
                getattr(config_provider, meth[0])(),
            )

