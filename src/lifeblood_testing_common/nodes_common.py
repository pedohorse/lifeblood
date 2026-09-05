import asyncio
from concurrent.futures import ThreadPoolExecutor
import os
import shutil
import tempfile
from pathlib import Path
import sqlite3
from unittest import mock, IsolatedAsyncioTestCase
import lifeblood
from lifeblood.attribute_serialization import serialize_attributes_core, deserialize_attributes_core
from lifeblood.enums import TaskState
from lifeblood.db_misc import sql_init_script
from lifeblood.basenode import BaseNode
from lifeblood.nodethings import ProcessingResult
from lifeblood.exceptions import NodeNotReadyToProcess
from lifeblood.scheduler.scheduler import Scheduler
from lifeblood_testing_common.common import create_default_scheduler
from lifeblood.worker import Worker
from lifeblood.invocationjob import Invocation, InvocationResources, Environment
from lifeblood.scheduler.pinger import Pinger
from lifeblood.pluginloader import PluginNodeDataProvider
from lifeblood.processingcontext import ProcessingContext
from lifeblood.process_utils import oh_no_its_windows
from lifeblood.environment_resolver import EnvironmentResolverArguments, BaseSimpleProcessSpawnEnvironmentResolver

from typing import Any, Callable, Dict, List, Mapping, Optional, Set, Union


# TODO: fix PluginNodeDataProvider's multiple instance problem and remove this global usage
plugin_data_provider = PluginNodeDataProvider(
    plugin_paths=(
        (Path(lifeblood.__file__).parent / 'stock_nodes', 'stock'),
        (Path(lifeblood.__file__).parent / 'core_nodes', 'core'),
    )
)


def create_node(node_type: str, node_name: str, scheduler, node_id):
    node = plugin_data_provider.node_factory(node_type)(node_name)
    node.set_parent(scheduler, node_id)
    return node


class FakeResolver(BaseSimpleProcessSpawnEnvironmentResolver):
    def __init__(self, path_to_bin: str):
        super().__init__()
        self.__bin_path = Path(path_to_bin)

    async def get_environment(self, arguments: Mapping) -> Environment:
        return Environment({**os.environ,
                            'PATH': os.pathsep.join((str(self.__bin_path), os.environ.get('PATH', ''))),
                            'PYTHONUNBUFFERED': '1'})


class FakeEnvArgs(EnvironmentResolverArguments):
    def __init__(self, path_to_bin: str):
        super().__init__()
        self.__bin_path = Path(path_to_bin)

    def get_resolver(self):
        return FakeResolver(self.__bin_path)


class PseudoTaskPool:
    def children_ids_for(self, task_id, active_only=False) -> List[int]:
        raise NotImplementedError()


class PseudoTask:
    def __init__(self, pool: PseudoTaskPool, task_id: int, attrs: dict, parent_id: Optional[int] = None, state: Optional[TaskState] = None, extra_fields: Optional[dict] = None):
        self.__id = task_id
        self.__parent_id = parent_id
        self.__state = state or TaskState.GENERATING
        self.__pool = pool
        self.__input_name = 'main'
        self.__task_dict = {
            'id': task_id,
            'node_input_name': self.__input_name,
            'state': self.__state.value,
            'parent_id': parent_id,
            'attributes':  serialize_attributes_core(attrs),
            **(extra_fields or {})
        }

    def id(self) -> int:
        return self.__id

    def parent_id(self) -> int:
        return self.__parent_id

    def state(self) -> TaskState:
        return self.__state

    def set_input_name(self, name: str):
        self.__input_name = name
        self.__task_dict['node_input_name'] = self.__input_name

    def get_context_for(self, node: BaseNode) -> ProcessingContext:
        return ProcessingContext(node.name(), node.label(), node.get_ui(), self.task_dict(), {})

    def task_dict(self) -> dict:
        return {**self.__task_dict, **{
            'children_count': len(self.__pool.children_ids_for(self.__id)),
            'active_children_count': len(self.__pool.children_ids_for(self.__id, True)),
        }}

    def attributes(self) -> dict:
        return deserialize_attributes_core(self.__task_dict['attributes'])

    def update_attribs(self, attribs_to_set: dict, attribs_to_delete: Optional[Set[str]] = None):
        attrs: dict = deserialize_attributes_core(self.__task_dict['attributes'])
        attrs.update(attribs_to_set)
        if attribs_to_delete:
            for attr in attribs_to_delete:
                attrs.pop(attr)
        self.__task_dict['attributes'] = serialize_attributes_core(attrs)

    def set_state(self, state: TaskState):
        self.__state = state
        self.__task_dict['state'] = self.__state.value

    def children(self, active: bool = False):
        return self.__pool


class PseudoContext(PseudoTaskPool):
    """
    sorta container for pseudo tasks and nodes for a single test
    """
    def __init__(self, scheduler: Scheduler):
        self.__scheduler = scheduler
        self.__last_node_id = 135  # cuz why starting with zero?
        self.__last_task_id = 468
        self.__last_split_id = 765
        self.__tasks: Dict[int, PseudoTask] = {}

    def create_pseudo_task_with_attrs(self, attrs: dict, task_id: Optional[int] = None, parent_id: Optional[int] = None, state: Optional[TaskState] = None, extra_fields: Optional[dict] = None) -> PseudoTask:
        if task_id is None:
            self.__last_task_id += 1
            task_id = self.__last_task_id
        task = PseudoTask(self, task_id, attrs, parent_id, state, extra_fields)
        self.__tasks[task_id] = task
        return task

    def create_pseudo_split_of(self, pseudo_task: PseudoTask, attribs_list):
        """
        split count will be taken from attrib_list's len
        """
        tasks = []
        self.__last_split_id += 1
        for i, attribs in enumerate(attribs_list):
            tasks.append(self.create_pseudo_task_with_attrs(attribs, task_id=None, parent_id=None, state=TaskState.GENERATING, extra_fields={
                'split_id': self.__last_split_id,
                'split_count': len(attribs_list),
                'split_origin_task_id': pseudo_task.id(),
                'split_element': i,
            }))
        return tasks

    def create_node(self, node_type: str, node_name: str) -> BaseNode:
        self.__last_node_id += 1
        return create_node(node_type, node_name, self.__scheduler, self.__last_node_id)

    def _update_attribs(self, task_id: int, update_attribs: dict, delete_attribs: set):
        if task_id not in self.__tasks:
            raise ValueError(f'something is wrong with the test - task_id {task_id} not found in pseudo context')
        self.__tasks[task_id].update_attribs(update_attribs, delete_attribs)

    def children_ids_for(self, task_id, active_only=False) -> List[int]:
        ids = []
        for tid, task in self.__tasks.items():
            if task.parent_id() == task_id and (not active_only or task.state() != TaskState.DEAD):
                ids.append(tid)
        return ids

    def id_to_task(self, task_id: int) -> PseudoTask:
        return self.__tasks[task_id]

    def process_task(self, node: BaseNode, task: PseudoTask, input_name='main') -> Optional[ProcessingResult]:
        """
        imitate normal processing.
        return None if processing raised not ready.
        other exceptions are propagated
        """
        try:
            res = node.process_task(task.get_context_for(node))
        except NodeNotReadyToProcess:
            return None
        task.update_attribs(res.attributes_to_set)
        return res

def purge_db():
    testdbpath = Path('test_nodes_common.db')
    if testdbpath.exists():
        testdbpath.unlink()
    testdbpath.touch()
    with sqlite3.connect('test_nodes_common.db') as con:
        con.executescript(sql_init_script)
    con.close()


class TestCaseBase(IsolatedAsyncioTestCase):
    async def _helper_test_worker_node(self,
                                       logic: Callable,
                                       *,
                                       task_done_logic: Optional[Callable] = None,
                                       runcode: Optional[str] = None,
                                       worker_count: int = 1,
                                       tasks_to_complete=None,
                                       expected_task_exit_code=0,
                                       **kwargs):
        """
        generic logic runner helper.
        this will start scheduler and worker,
        then it will run provided "logic",

        logic(sched, workers, tmp_script_path, done_waiter)
        will be provided with
        - the instance of scheduler
        - list of running workers
        - prepared temp path to store scripts in (will be cleaned after) with given `runcode`
        - event that will trigger when "task done" is reported by the worker

        """
        purge_db()
        with mock.patch('lifeblood.scheduler.scheduler_core.Pinger') as ppatch, \
             mock.patch('lifeblood.worker.Worker.scheduler_pinger') as wppatch:

            ppatch.return_value = mock.AsyncMock(Pinger)
            wppatch.return_value = mock.AsyncMock()

            sched = create_default_scheduler('test_nodes_common.db', do_broadcasting=False, helpers_minimal_idle_to_ensure=0, **kwargs)
            await sched.start()

            workers = []
            for i in range(worker_count):
                worker = Worker(
                    sched.server_message_addresses()[0],
                    scheduler_ping_interval=9001,
                )
                await worker.start()
                workers.append(worker)

            await asyncio.gather(sched.wait_till_starts(),
                                 *[worker.wait_till_starts() for worker in workers])

            #
            fd, tmp_script_path = None, None
            if runcode:
                fd, tmp_script_path = tempfile.mkstemp('.py')
            try:
                if tmp_script_path:
                    with open(tmp_script_path, 'w') as f:
                        f.write(runcode)

                done_ev = asyncio.Event()
                tasks_to_complete = tasks_to_complete or worker_count
                side_effect_was_good = True
                with mock.patch('lifeblood.scheduler.scheduler.Scheduler.task_done_reported') as td_patch:
                    def _side_effect(task: Invocation, stdout: str, stderr: str):
                        nonlocal tasks_to_complete, side_effect_was_good
                        tasks_to_complete -= 1
                        print(f'finished {task.task_id()} out: {stdout}')
                        print(f'finished {task.task_id()} err: {stderr}')
                        print(f'exit code: {task.exit_code()}')
                        side_effect_was_good = side_effect_was_good and expected_task_exit_code == task.exit_code()
                        if task_done_logic:
                            try:
                                task_done_logic(task)
                            except Exception as e:
                                side_effect_was_good = False
                                print(e)
                        if tasks_to_complete <= 0:
                            done_ev.set()

                    td_patch.side_effect = _side_effect
                    done_waiter = asyncio.create_task(done_ev.wait())
                    await logic(sched, workers, tmp_script_path, done_waiter)
                    if not done_waiter.done():
                        done_waiter.cancel()
                    self.assertTrue(side_effect_was_good)
            finally:
                if tmp_script_path:
                    os.close(fd)
                    os.unlink(tmp_script_path)

                for worker in workers:
                    worker.stop()
                    await worker.wait_till_stops()
                sched.stop()
                await sched.wait_till_stops()

    async def _helper_test_node_with_arg_update(self,
                                                logic: Callable):
        """
        logic
        logic(sched, workers, done_waiter, context)
        """

        async def _logic(sched: Scheduler, workers: List[Worker], tmp_script_path: str, done_waiter: asyncio.Event):
            context = PseudoContext(sched)
            with mock.patch('lifeblood.scheduler.scheduler.Scheduler.update_task_attributes') as attr_patch:
                attr_patch.side_effect = lambda task_id, attributes_to_update, attributes_to_delete, *args, **kwargs: \
                    (
                        context._update_attribs(task_id, attributes_to_update, attributes_to_delete)
                    )
                return await logic(sched, workers, done_waiter, context)

        return await self._helper_test_worker_node(_logic)

    async def _helper_test_render_node(
            self,
            node_type_name,
            scn_ext,
            command,
            bin_rel_path,
            special_task_done_logic=None,
            skip_outfile_check=False,
            expected_task_exit_code=0,
            **kwargs
    ):
        """
        kwargs are forwarded to _helper_test_worker_node (which forwards it to scheduler constructor
        """
        the_worker = None
        tmpdir = tempfile.mkdtemp(prefix='test_render_')
        try:
            out_exr_path = os.path.join(tmpdir, f'test_render_foooo_{node_type_name}.exr')
            if os.path.exists(out_exr_path):
                os.unlink(out_exr_path)

            def _logic_gen(skip_existing: bool):
                async def _logic(scheduler, workers, tmp_script_path, done_waiter):
                    nonlocal the_worker
                    updated_attrs = {}
                    with mock.patch('lifeblood.scheduler.scheduler.Scheduler.update_task_attributes') as attr_patch:
                        attr_patch.side_effect = lambda *args, **kwargs: updated_attrs.update(args[1]) \
                                                                         or print(f'update_task_attributes with {args}, {kwargs}')

                        out_preexists = os.path.exists(out_exr_path)
                        pre_contents = None
                        if out_preexists:
                            with open(out_exr_path, 'rb') as f:
                                pre_contents = f.read()

                        node = create_node(node_type_name, f'test {node_type_name}', scheduler, 1)
                        node.set_param_value('skip if exists', skip_existing)

                        scn_filepath = f'/tmp/something/karma/filename.1234.{scn_ext}'
                        start_attrs = {
                            'file': scn_filepath,
                            'outimage': out_exr_path,
                            'frames': [1, 2, 3]
                        }
                        res = node.process_task(ProcessingContext(node.name(), node.label(), node.get_ui(), {'attributes': serialize_attributes_core(start_attrs)}, {}))

                        ij = res.invocation_job
                        self.assertTrue(ij is not None)
                        ij._set_envresolver_arguments(FakeEnvArgs(bin_rel_path))
                        if oh_no_its_windows:
                            # cuz windows does not allow to just run batch/python scripts with no extension...
                            for filename, contents in list(ij.extra_files().items()):
                                ij.set_extra_file(filename, contents.replace(f"'{command}'", f"'python', {repr(str(Path(__file__).parent / Path(bin_rel_path) / command))}"))

                            if ij.args()[0] == command:
                                ij.args().pop(0)
                                ij.args().insert(0, str(Path(__file__).parent / Path(bin_rel_path) / command))
                                ij.args().insert(0, 'python')

                        invoc = Invocation(
                            ij,
                            invocation_id=1234,
                            task_id=2345,
                            resources_to_use=InvocationResources({}, {}),
                        )
                        the_worker = workers[0]

                        await workers[0].run_task(
                            invoc,
                            scheduler.server_message_addresses()[0]
                        )

                        await asyncio.wait([done_waiter], timeout=30)

                        # now postprocess task
                        res = node.postprocess_task(ProcessingContext(node.name(), node.label(), node.get_ui(), {'attributes': serialize_attributes_core({
                            **start_attrs,
                            **updated_attrs
                        })}, {}))
                        if res.attributes_to_set:
                            updated_attrs.update(res.attributes_to_set)

                        if not skip_outfile_check:
                            self.assertTrue(os.path.exists(out_exr_path))
                            self.assertEqual(out_exr_path, updated_attrs.get('file'))

                            with open(out_exr_path, 'rb') as f:
                                post_contents = f.read()
                            if pre_contents is not None and skip_existing:
                                self.assertEqual(pre_contents, post_contents)
                            if pre_contents is None or not skip_existing:
                                line_ok, line_args, line_fname = post_contents.splitlines(keepends=False)
                                self.assertEqual(b'ok', line_ok)
                                self.assertEqual(scn_filepath.encode(), line_fname)

                return _logic

            def _task_done_logic(task: Invocation):
                if special_task_done_logic:
                    special_task_done_logic(the_worker, task)
                else:
                    self.assertEqual(100.0, the_worker.task_status())

            for skip_exist, pre_exist in ((False, False), (True, False), (True, True)):
                print(f'testing with: skip_exist={skip_exist}')

                self.assertFalse(os.path.exists(out_exr_path))
                if pre_exist:
                    os.makedirs(os.path.dirname(out_exr_path), exist_ok=True)
                    with open(out_exr_path, 'w') as f:
                        f.write('some preexisting contents that is not the same as husk mock outputs')
                await self._helper_test_worker_node(
                    _logic_gen(skip_exist),
                    task_done_logic=_task_done_logic if not skip_exist or not pre_exist else lambda *args, **kwargs: None,
                    expected_task_exit_code=expected_task_exit_code if not skip_exist or not pre_exist else 0,
                    **kwargs
                )
                if os.path.exists(out_exr_path):
                    os.unlink(out_exr_path)
        finally:
            shutil.rmtree(tmpdir)

    async def _helper_test_simple_invocation(
            self,
            node_type_to_create: str,
            node_params_to_set: List[Dict[str, Any]],
            task_attrs: Dict[str, Any],
            *,
            add_relative_to_PATH: Optional[Union[str, Path]] = None,
            commands_to_replace_with_py_mock: List[str] = None,
            expected_task_exit_code: int = 0,
    ):
        """
        helper for most general node testing:

        give it preparation logic, what node to create, what parameters to set, what attributes should task have

        add_relative_to_PATH - add that to PATH, relative to tests dir
        command_to_replace_with_py_mock - if you mocked some command with some python script - a hack is needed on windows to run it.
            it's a nasty and dirty hack that replaces "'command'" with "'python', 'command'" in script files...
            maybe too specific for all cases
        """
        updated_attrs = {}

        async def _logic(scheduler, workers, script_path, done_waiter):
            with mock.patch('lifeblood.scheduler.scheduler.Scheduler.update_task_attributes') as attr_patch:
                attr_patch.side_effect = lambda *args, **kwargs: updated_attrs.update(args[1]) \
                                                                 or print(f'update_task_attributes with {args}, {kwargs}')
                node = create_node(node_type_to_create, f'test {node_type_to_create}', scheduler, 1)

                # it's a list of dicts cuz sometimes we need strict ordering of sets
                for params in node_params_to_set:
                    for param, val in params.items():
                        node.set_param_value(param, val)

                pool = ThreadPoolExecutor(max_workers=1)
                res = await asyncio.get_event_loop().run_in_executor(pool, node.process_task, ProcessingContext(node.name(), node.label(), node.get_ui(), {'id': 1, 'attributes': serialize_attributes_core(task_attrs)}, {}))
                if res.attributes_to_set:
                    updated_attrs.update(res.attributes_to_set)

                ij = res.invocation_job
                self.assertTrue(ij is not None)
                if add_relative_to_PATH:
                    ij._set_envresolver_arguments(FakeEnvArgs(add_relative_to_PATH))

                if oh_no_its_windows:
                    # cuz windows does not allow to just run batch/python scripts with no extension...
                    for command in commands_to_replace_with_py_mock:
                        for filename, contents in list(ij.extra_files().items()):
                            ij.set_extra_file(filename, contents.replace(f"'{command}'", f"'python', {repr(str(Path(__file__).parent / Path(add_relative_to_PATH) / command))}"))

                        if ij.args()[0] == command:
                            ij.args().pop(0)
                            ij.args().insert(0, str(Path(__file__).parent / Path(add_relative_to_PATH) / command))
                            ij.args().insert(0, 'python')

                invoc = Invocation(
                    ij,
                    invocation_id=1234,
                    task_id=2345,
                    resources_to_use=InvocationResources({}, {}),
                )

                the_worker = workers[0]

                await workers[0].run_task(
                    invoc,
                    scheduler.server_message_addresses()[0]
                )

                await asyncio.wait([done_waiter], timeout=30)

                # now postprocess task
                res = node.postprocess_task(ProcessingContext(node.name(), node.label(), node.get_ui(), {'attributes': serialize_attributes_core({
                    **task_attrs,
                    **updated_attrs
                })}, {}))
                if res.attributes_to_set:
                    updated_attrs.update(res.attributes_to_set)

        await self._helper_test_worker_node(_logic, expected_task_exit_code=expected_task_exit_code)

        return updated_attrs
