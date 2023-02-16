import asyncio
import socket
import struct
import json
import time
import pickle
from io import BytesIO

from lifeblood.uidata import NodeUi
from lifeblood.ui_protocol_data import UiData
from lifeblood.invocationjob import InvocationJob
from lifeblood.nethelpers import recv_exactly, address_to_ip_port, get_default_addr
from lifeblood import logging
from lifeblood.enums import NodeParameterType, TaskState, TaskGroupArchivedState
from lifeblood.broadcasting import await_broadcast
from lifeblood.config import get_config
from lifeblood.uidata import Parameter
from lifeblood.net_classes import NodeTypeMetadata
from lifeblood.taskspawn import NewTask
from lifeblood.snippets import NodeSnippetData, NodeSnippetDataPlaceholder
from lifeblood.defaults import ui_port
from lifeblood.environment_resolver import EnvironmentResolverArguments
from lifeblood.scheduler_ui_protocol import UIProtocolSocketClient

import PySide2
from PySide2.QtCore import Signal, Slot, QPointF, QThread
#from PySide2.QtGui import QPoin

from typing import Optional, Set, List, Union, Dict, Iterable


logger = logging.get_logger('viewer')


class SchedulerConnectionWorker(PySide2.QtCore.QObject):
    full_update = Signal(object)
    bd_uid_update = Signal(object)
    graph_update = Signal(object)
    tasks_update = Signal(object)
    groups_update = Signal(object)
    workers_update = Signal(object)

    log_fetched = Signal(int, dict)
    nodeui_fetched = Signal(int, NodeUi)
    task_attribs_fetched = Signal(int, tuple, object)
    task_invocation_job_fetched = Signal(int, InvocationJob)
    nodetypes_fetched = Signal(dict)
    nodepresets_fetched = Signal(dict)
    nodepreset_fetched = Signal(str, str, NodeSnippetData, object)
    node_has_parameter = Signal(int, str, bool, object)
    node_parameter_changed = Signal(int, Parameter, object, object)
    node_parameters_changed = Signal(int, object, object, object)
    node_parameter_expression_changed = Signal(int, Parameter, object)
    node_settings_applied = Signal(int, str, object)
    node_custom_settings_saved = Signal(str, str, object)
    node_default_settings_set = Signal(str, object, object)
    node_created = Signal(int, str, str, QPointF, object)
    nodes_copied = Signal(dict, QPointF)

    def __init__(self, parent=None):
        super(SchedulerConnectionWorker, self).__init__(parent)
        self.__started = False
        self.__timer = None
        self.__to_stop = False
        self.__task_group_filter: Optional[Set[str]] = None
        # self.__conn: Optional[socket.socket] = None
        # self.__filter_dead = True
        self.__skip_dead = False
        self.__skip_archived_groups = True

        self.__latest_graph_update_id = -1
        self.__last_groups_checked_timestamp = 0

        self.__client: Optional[UIProtocolSocketClient] = None

    def request_interruption(self):
        self.__to_stop = True  # assume it's atomic, which it should be

    def interruption_requested(self):
        return self.__to_stop

    @Slot()
    def start(self):
        """
        supposed to be called from the thread timer lives in
        starts checking on full state
        :return:
        """
        assert self.thread() == QThread.currentThread()
        self.__started = True
        self.__timer = PySide2.QtCore.QTimer(self)
        self.__timer.setInterval(1000)
        self.__timer.timeout.connect(self.check_scheduler)
        self.__timer.start()

    @Slot()
    def finish(self):
        """
        note that interruption mush have been requested before
        after this out thread will probably never enter the event loop again
        :return:
        """
        self.__timer.stop()

    @Slot(set)
    def set_task_group_filter(self, groups: Set[str]):
        self.__task_group_filter = groups
        # TODO: force update

    def ensure_connected(self) -> bool:
        if self.__client is not None:
            return True

        async def _interrupt_waiter():
            while True:
                if self.interruption_requested():
                    return None
                await asyncio.sleep(0.5)

        config = get_config('viewer')
        if config.get_option_noasync('viewer.listen_to_broadcast', True):
            sche_addr, sche_port = None, None
            # check last known address first
            lastaddr = config.get_option_noasync('viewer.last_scheduler_address', None)
            if lastaddr is not None:
                sche_addr, sche_port = lastaddr.split(':')
                sche_port = int(sche_port)
            if sche_addr is not None:
                logger.info(f'trying to connect to the last known scheduler\'s address {sche_addr}:{sche_port}')
                tmp_sock = None
                try:
                    tmp_sock = socket.create_connection((sche_addr, sche_port), timeout=5)
                    tmp_sock.sendall(b'\0\1\0\0')
                except ConnectionError:
                    logger.info('last known address didn\'t work')
                    sche_addr, sche_port = None, None
                finally:
                    if tmp_sock is not None:
                        tmp_sock.close()

            if sche_addr is None:
                logger.info('waiting for scheduler broadcast...')
                tasks = asyncio.run(asyncio.wait((
                    await_broadcast('lifeblood_scheduler'),
                    _interrupt_waiter()), return_when=asyncio.FIRST_COMPLETED))

                logger.debug(tasks)
                message = list(tasks[0])[0].result()

                logger.debug(message)
                if message is None:
                    return False
                logger.debug('received broadcast: %s', message)
                schedata = json.loads(message)

                sche_addr, sche_port = address_to_ip_port(schedata['ui'])  #schedata['ui'].split(':')
                #sche_port = int(sche_port)
        else:
            sche_addr = config.get_option_noasync('viewer.scheduler_ip', get_default_addr())
            sche_port = config.get_option_noasync('viewer.scheduler_port', ui_port())
        logger.debug(f'connecting to scheduler on {sche_addr}:{sche_port} ...')

        while not self.interruption_requested():
            try:
                self.__client = UIProtocolSocketClient(sche_addr, sche_port, timeout=30)
                bd_uid_update = self.__client.get_db_uid()
                self.bd_uid_update.emit(bd_uid_update)
            except ConnectionError:
                logger.debug('ui connection refused, retrying...')

                # now sleep, but listening to interrupt requests
                for i in range(25):
                    time.sleep(0.2)
                    if self.interruption_requested():
                        return False
            else:
                break

        assert self.__client is not None
        self.__client.initialize()
        config.set_option_noasync('viewer.last_scheduler_address', f'{sche_addr}:{sche_port}')
        return True

    def skip_dead(self) -> bool:
        return self.__skip_dead

    def skip_archived_groups(self) -> bool:
        return self.__skip_archived_groups

    @Slot(bool)
    def set_skip_dead(self, do_skip: bool) -> None:
        self.__skip_dead = do_skip

    @Slot(bool)
    def set_skip_archived_groups(self, do_skip: bool) -> None:
        self.__skip_archived_groups = do_skip

    @Slot()
    def check_scheduler(self):
        if self.interruption_requested():
            self.__timer.stop()
            if self.__client is not None:
                self.__client.close()
            self.__client = None
            return

        if not self.ensure_connected():
            return

        assert self.__client is not None

        try:
            latest_id = self.__client.get_ui_graph_state_update_id()
            if latest_id > self.__latest_graph_update_id:
                graph_state, self.__latest_graph_update_id = self.__client.get_ui_graph_state()
                self.graph_update.emit(graph_state)

            tasks_state = self.__client.get_ui_tasks_state(self.__task_group_filter, not self.__skip_dead)
            self.tasks_update.emit(tasks_state)

            workers_state = self.__client.get_ui_workers_state()
            self.workers_update.emit(workers_state)

            if time.time() - self.__last_groups_checked_timestamp > 5.0:  # TODO: to config with it!
                groups_state = self.__client.get_ui_task_groups(self.__skip_archived_groups)
                self.__last_groups_checked_timestamp = time.time()
                self.groups_update.emit(groups_state)

        except ConnectionError as e:
            logger.error(f'connection reset {e}')
            logger.error('scheduler connection lost')
            self.__client = None
            return
        except Exception:
            logger.exception('problems in network operations')
            self.__client = None
            return

    @Slot(int)
    def get_invocation_metadata(self, task_id: int):
        if not self.ensure_connected():
            return

        assert self.__client is not None
        try:
            invocmeta = self.__client.get_invoc_meta(task_id)
        except ConnectionError as e:
            logger.error(f'failed {e}')
        except Exception:
            logger.exception('problems in network operations')
        else:
            self.log_fetched.emit(task_id, invocmeta)

    @Slot(int, object)
    def get_task_attribs(self, task_id: int, data=None):
        if not self.ensure_connected():
            return
        assert self.__client is not None

        try:
            attrs, env_attrs = self.__client.get_task_attribs(task_id)
        except ConnectionError as e:
            logger.error(f'failed {e}')
        except Exception:
            logger.exception('problems in network operations')
        else:
            self.task_attribs_fetched.emit(task_id, (attrs, env_attrs), data)

    @Slot(int)
    def get_task_invocation_job(self, task_id: int):
        if not self.ensure_connected():
            return
        assert self.__client is not None

        try:
            invoc = self.__client.get_task_invocation(task_id)
        except ConnectionError as e:
            logger.error(f'failed {e}')
        except Exception:
            logger.exception('problems in network operations')
        else:
            self.task_invocation_job_fetched.emit(task_id, invoc)

    @Slot(int, int, int)
    def get_log(self, task_id: int, node_id: int, invocation_id: int):
        if not self.ensure_connected():
            return

        assert self.__client is not None
        try:
            alllogs = self.__client.get_log(task_id, node_id, invocation_id)
        except ConnectionError as e:
            logger.error(f'failed {e}')
        except Exception:
            logger.exception('problems in network operations')
        else:
            self.log_fetched.emit(task_id, alllogs)

    @Slot()
    def get_nodeui(self, node_id: int):
        if not self.ensure_connected():
            return
        assert self.__client is not None

        try:
            nodeui = self.__client.get_node_interface(node_id)
        except ConnectionError as e:
            logger.error(f'failed {e}')
        except Exception:
            logger.exception('problems in network operations')
        else:
            self.nodeui_fetched.emit(node_id, nodeui)

    @Slot()
    def send_node_has_parameter(self, node_id: int, param_name: str, data=None):
        if not self.ensure_connected():
            return
        assert self.__client is not None

        try:
            good = self.__client.node_has_param(node_id, param_name)
        except ConnectionError as e:
            logger.error(f'failed {e}')
        except Exception:
            logger.exception('problems in network operations')
        else:
            self.node_has_parameter.emit(node_id, param_name, good, data)

    @Slot()
    def send_node_parameter_change(self, node_id: int, param: Parameter, data=None):
        if not self.ensure_connected():
            return
        assert self.__client is not None

        try:
            new_val = self.__client.set_node_param(node_id, param)
        except ConnectionError as e:
            logger.error(f'failed {e}')
        except Exception:
            logger.exception('problems in network operations')
        else:
            self.node_parameter_changed.emit(node_id, param, new_val, data)

    @Slot()
    def send_node_parameters_change(self, node_id: int, params: Iterable[Parameter], data=None):
        if not self.ensure_connected():
            return
        assert self.__client is not None

        try:
            self.__client.set_node_params(node_id, params, want_result=False)
        except ConnectionError as e:
            logger.error(f'failed {e}')
        except Exception:
            logger.exception('problems in network operations')
        else:
            self.node_parameters_changed.emit(node_id, tuple(params), None, data)

    @Slot()
    def send_node_parameter_expression_change(self, node_id: int, param: Parameter, data=None):
        if not self.ensure_connected():
            return
        assert self.__client is not None

        try:
            raise DeprecationWarning('removing this shit')
            # set_or_unset = param.has_expression()
            # self.__conn.sendall(b'setnodeparamexpression\n')
            # self.__conn.sendall(struct.pack('>Q?', node_id, set_or_unset))
            # self._send_string(param.name())
            # if set_or_unset:
            #     expression = param.expression()
            #     self._send_string(expression)
            # assert recv_exactly(self.__conn, 1) == b'\1'
            self.node_parameter_expression_changed.emit(node_id, param, data)
        except ConnectionError as e:
            logger.error(f'failed {e}')
        except Exception:
            logger.exception('problems in network operations')

    @Slot()
    def apply_node_settings(self, node_id: int, settings_name: str, data):
        if not self.ensure_connected():
            return
        assert self.__client is not None

        try:
            self.__client.apply_node_settings(node_id, settings_name)
        except ConnectionError as e:
            logger.error(f'failed {e}')
        except Exception:
            logger.exception('problems in network operations')
        else:
            self.node_settings_applied.emit(node_id, settings_name, data)

    @Slot()
    def node_save_custom_settings(self, node_type_name: str, settings_name: str, settings: dict, data):
        if not self.ensure_connected():
            return
        assert self.__client is not None

        try:
            self.__client.save_custom_node_settings(node_type_name, settings_name, settings)
        except ConnectionError as e:
            logger.error(f'failed {e}')
        except Exception:
            logger.exception('problems in network operations')
        else:
            self.node_custom_settings_saved.emit(node_type_name, settings_name, data)

    @Slot()
    def node_set_settings_default(self, node_type_name: str, settings_name: Optional[str], data):
        if not self.ensure_connected():
            return
        assert self.__client is not None

        try:
            self.__client.set_settings_default(node_type_name, settings_name)
        except ConnectionError as e:
            logger.error(f'failed {e}')
        except Exception:
            logger.exception('problems in network operations')
        else:
            self.node_default_settings_set.emit(node_type_name, settings_name, data)

    @Slot()
    def get_nodetypes(self):
        if not self.ensure_connected():
            return
        assert self.__client is not None

        try:
            nodetypes = self.__client.list_node_types()
        except ConnectionError as e:
            logger.error(f'failed {e}')
        except Exception:
            logger.exception('problems in network operations')
        else:
            self.nodetypes_fetched.emit(nodetypes)

    @Slot()
    def get_nodepresets(self):
        if not self.ensure_connected():
            return
        assert self.__client is not None

        try:
            presets = self.__client.list_presets()
        except ConnectionError as e:
            logger.error(f'failed {e}')
        except Exception:
            logger.exception('problems in network operations')
        else:
            self.nodepresets_fetched.emit(presets)

    @Slot(str, str)
    def get_nodepreset(self, package: str, preset: str, data=None):  # TODO: rename these two functions (this and up)
        if not self.ensure_connected():
            return
        assert self.__client is not None

        try:
            snippet = self.__client.get_node_preset(package)
            if snippet is None:
                return
        except ConnectionError as e:
            logger.error(f'failed {e}')
        except Exception:
            logger.exception('problems in network operations')
        else:
            self.nodepreset_fetched.emit(package, preset, snippet, data)

    @Slot()
    def create_node(self, node_type, node_name, pos, data=None):
        """
        create a new node of given time

        :param node_type:
        :param node_name:
        :param pos: just pass through # TODO: maybe better unite it with data?
        :param data: arbitrary data to pass through, may be used to mark this operation
        :return:
        """
        if not self.ensure_connected():
            return
        assert self.__client is not None

        try:
            node_id = self.__client.add_node(node_type, node_name)
        except ConnectionError as e:
            logger.error(f'failed {e}')
        except Exception:
            logger.exception('problems in network operations')
        else:
            self.node_created.emit(node_id, node_type, node_name, pos, data)

    @Slot()
    def remove_node(self, node_id: int):
        if not self.ensure_connected():
            return
        assert self.__client is not None

        try:
            self.__client.remove_node(node_id)
        except ConnectionError as e:
            logger.error(f'failed {e}')
        except Exception:
            logger.exception('problems in network operations')

    @Slot()
    def wipe_node(self, node_id: int):
        if not self.ensure_connected():
            return
        assert self.__client is not None

        try:
            self.__client.wipe_node(node_id)
        except ConnectionError as e:
            logger.error(f'failed {e}')
        except Exception:
            logger.exception('problems in network operations')

    @Slot()
    def set_node_name(self, node_id: int, node_name: str):
        if not self.ensure_connected():
            return
        assert self.__client is not None

        try:
            self.__client.rename_node(node_id, node_name)
        except ConnectionError as e:
            logger.error(f'failed {e}')
        except Exception:
            logger.exception('problems in network operations')

    @Slot()
    def duplicate_nodes(self, node_ids: List[int], shift: QPointF):
        if not self.ensure_connected():
            return
        assert self.__client is not None

        try:
            ret = self.__client.duplicate_nodes(node_ids)
            self.nodes_copied.emit(ret, shift)
        except ConnectionError as e:
            logger.error(f'failed {e}')
        except Exception:
            logger.exception('problems in network operations')

    @Slot()
    def change_node_connection(self, connection_id: int, outnode_id: Optional[int] = None, outname: Optional[str] = None, innode_id: Optional[int] = None, inname: Optional[str] = None):
        if not self.ensure_connected():
            return
        assert self.__client is not None

        try:
            logger.debug(f'{connection_id}, {outnode_id}, {outname}, {innode_id}, {inname}')
            self.__client.change_connection_by_id(connection_id, outnode_id, outname, innode_id, inname)
        except ConnectionError as e:
            logger.error(f'failed {e}')
        except Exception:
            logger.exception('problems in network operations')

    @Slot()
    def add_node_connection(self, outnode_id: int, outname: str, innode_id: int, inname: str):
        if not self.ensure_connected():
            return
        assert self.__client is not None

        try:
            new_id = self.__client.add_connection(outnode_id, outname, innode_id, inname)
            # TODO: need a signal for this shit
        except ConnectionError as e:
            logger.error(f'failed {e}')
        except Exception:
            logger.exception('problems in network operations')

    @Slot()
    def remove_node_connection(self, connection_id: int):
        if not self.ensure_connected():
            return
        assert self.__client is not None

        try:
            self.__client.remove_connection_by_id(connection_id)
        except ConnectionError as e:
            logger.error(f'failed {e}')
        except Exception:
            logger.exception('problems in network operations')

    # task control things
    @Slot()
    def set_tasks_paused(self, task_ids_or_group: Union[List[int], int, str], paused: bool):
        if len(task_ids_or_group) == 0:
            return
        if not self.ensure_connected():
            return
        assert self.__client is not None

        try:
            if isinstance(task_ids_or_group, str):
                self.__client.pause_task_group(task_ids_or_group, paused)
            else:
                if isinstance(task_ids_or_group, int):
                    task_ids_or_group = [task_ids_or_group]
                self.__client.pause_tasks(task_ids_or_group)
        except ConnectionError as e:
            logger.error(f'failed {e}')
        except Exception:
            logger.exception('problems in network operations')

    @Slot()
    def set_task_group_archived_state(self, task_group_name: str, state: TaskGroupArchivedState):
        if not self.ensure_connected():
            return
        assert self.__client is not None

        try:
            self.__client.archive_task_group(state, task_group_name)
        except ConnectionError as e:
            logger.error(f'failed {e}')
        except Exception:
            logger.exception('problems in network operations')

    @Slot()
    def set_task_node(self, task_id: int, node_id: int):
        if not self.ensure_connected():
            return
        assert self.__client is not None

        try:
            self.__client.set_node_for_task(task_id, node_id)
        except ConnectionError as e:
            logger.error(f'failed {e}')
        except Exception:
            logger.exception('problems in network operations')

    @Slot()
    def set_task_state(self, task_ids: List[int], state: TaskState):
        numtasks = len(task_ids)
        if numtasks == 0:
            return
        if not self.ensure_connected():
            return
        assert self.__client is not None

        try:
            self.__client.change_tasks_state(task_ids, state)
        except ConnectionError as e:
            logger.error(f'failed {e}')
        except Exception:
            logger.exception('problems in network operations')

    @Slot()
    def set_task_name(self, task_id: int, name: str):
        if not self.ensure_connected():
            return
        assert self.__client is not None

        try:
            self.__client.set_task_name(task_id, name)
        except ConnectionError as e:
            logger.error(f'failed {e}')
        except Exception:
            logger.exception('problems in network operations')

    @Slot()
    def set_task_groups(self, task_id: int, groups: set):
        if not self.ensure_connected():
            return
        assert self.__client is not None

        try:
            self.__client.set_task_groups(task_id, groups)
        except ConnectionError as e:
            logger.error(f'failed {e}')
        except Exception:
            logger.exception('problems in network operations')

    @Slot()
    def update_task_attributes(self, task_id: int, attribs_to_set: dict, attribs_to_delete: set):
        if not self.ensure_connected():
            return
        assert self.__client is not None

        try:
            self.__client.update_task_attributes(task_id, attribs_to_set, attribs_to_delete)
        except ConnectionError as e:
            logger.error(f'failed {e}')
        except Exception:
            logger.exception('problems in network operations')

    @Slot()
    def cancel_task(self, task_id: int):
        if not self.ensure_connected():
            return
        assert self.__client is not None

        try:
            self.__client.cancel_invocation_for_task(task_id)
        except ConnectionError as e:
            logger.error(f'failed {e}')
        except Exception:
            logger.exception('problems in network operations')

    @Slot(object)
    def cancel_task_for_worker(self, worker_id: int):
        if not self.ensure_connected():
            return
        assert self.__client is not None

        try:
            self.__client.cancel_invocation_for_worker(worker_id)
        except ConnectionError as e:
            logger.error(f'failed {e}')
        except Exception:
            logger.exception('problems in network operations')

    @Slot()
    def add_task(self, new_task: NewTask):
        if not self.ensure_connected():
            return
        assert self.__client is not None

        try:
            self.__client.add_task(new_task)
        except ConnectionError as e:
            logger.error(f'failed {e}')
        except Exception:
            logger.exception('problem in network operations')

    @Slot()
    def set_environment_resolver_arguments(self, task_id: int, env_args: Optional[EnvironmentResolverArguments]):
        if not self.ensure_connected():
            return
        assert self.__client is not None

        try:
            self.__client.set_task_environment_resolver_arguments(task_id, env_args)
        except ConnectionError as e:
            logger.error(f'failed {e}')
        except Exception:
            logger.exception('problem in network operations')

    @Slot()
    def unset_environment_resolver_arguments(self, task_id: int):
        return self.set_environment_resolver_arguments(task_id, None)

    # TODO: problem below will affect ALL 64 bit arguments, need to correct all other functions
    @Slot(object, list)  # interestingly since int here is 64 bit - i have to mark signal as object, but then it doesn't connect unless i specify slot as object too.
    def set_worker_groups(self, whwid: int, groups: List[str]):
        logger.debug(f'set_worker_groups with {whwid}, {groups}')
        if not self.ensure_connected():
            return
        assert self.__client is not None

        try:
            self.__client.set_worker_groups(whwid, groups)
        except ConnectionError as e:
            logger.error(f'failed {e}')
        except Exception:
            logger.exception('problem in network operations')
