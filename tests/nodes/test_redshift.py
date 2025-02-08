from pathlib import Path

from lifeblood_testing_common.nodes_common import TestCaseBase
from lifeblood.invocationjob import Invocation
from lifeblood.worker_core import WorkerCore
from lifeblood.worker_resource_definition import WorkerResourceDefinition, WorkerDeviceTypeDefinition


class RedshiftTestCase(TestCaseBase):
    async def test_redshift_node_no_gpu_defined(self):
        await self._helper_test_render_node(
            'redshift',
            'rs',
            'redshiftCmdLine',
            Path(__file__).parent / 'data' / 'mock_redshift',
            device_type_definitions=(),
        )

        # TODO: add check for 'files' attr (when redshift node is fixed to be able to set them on "skip existing")

    async def test_redshift_node_gpu_defined_but_not_provided(self):
        # when gpu is defined, but no device is provided to the invocation - error must be thrown
        def done_logic(worker: WorkerCore, task: Invocation):
            self.assertEqual(1, task.exit_code())

        await self._helper_test_render_node(
            'redshift',
            'rs',
            'redshiftCmdLine',
            Path(__file__).parent / 'data' / 'mock_redshift',
            special_task_done_logic=done_logic,
            skip_outfile_check=True,
            expected_task_exit_code=1,
            device_type_definitions=(WorkerDeviceTypeDefinition('gpu', ()),),
        )
