import asyncio
import socket
import struct
import json
import time
import pickle

from lifeblood.uidata import UiData, NodeUi
from lifeblood.invocationjob import InvocationJob
from lifeblood.nethelpers import recv_exactly, address_to_ip_port, get_default_addr
from lifeblood import logging
from lifeblood.enums import NodeParameterType, TaskState, TaskGroupArchivedState
from lifeblood.broadcasting import await_broadcast
from lifeblood.config import get_config
from lifeblood.uidata import Parameter
from lifeblood.net_classes import NodeTypeMetadata
from lifeblood.taskspawn import NewTask
from lifeblood.snippets import NodeSnippetData
from lifeblood.defaults import ui_port
from lifeblood.environment_resolver import EnvironmentResolverArguments

import PySide2
from PySide2.QtCore import Signal, Slot, QPointF, QThread
#from PySide2.QtGui import QPoin

from typing import Optional, Set, List, Union, Dict


logger = logging.get_logger('viewer')


class SchedulerConnectionWorker(PySide2.QtCore.QObject):
    full_update = Signal(UiData)
    log_fetched = Signal(int, dict)
    nodeui_fetched = Signal(int, NodeUi)
    task_attribs_fetched = Signal(int, tuple, object)
    task_invocation_job_fetched = Signal(int, InvocationJob)
    nodetypes_fetched = Signal(dict)
    nodepresets_fetched = Signal(dict)
    nodepreset_fetched = Signal(str, str, NodeSnippetData, object)
    node_has_parameter = Signal(int, str, bool, object)
    node_parameter_changed = Signal(int, Parameter, object, object)
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
        self.__conn: Optional[socket.socket] = None
        # self.__filter_dead = True
        self.__skip_dead = False
        self.__skip_archived_groups = True

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
        if self.__conn is not None:
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
                    tmp_sock.sendall(b'\0\0\0\0')
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
                self.__conn = socket.create_connection((sche_addr, sche_port), timeout=30)
            except ConnectionError:
                logger.debug('ui connection refused, retrying...')

                # now sleep, but listening to interrupt requests
                for i in range(25):
                    time.sleep(0.2)
                    if self.interruption_requested():
                        return False
            else:
                break

        assert self.__conn is not None
        self.__conn.sendall(b'\0\0\0\0')
        config.set_option_noasync('viewer.last_scheduler_address', f'{sche_addr}:{sche_port}')
        return True

    def _send_string(self, text: str):
        bts = text.encode('UTF-8')
        self.__conn.sendall(struct.pack('>Q', len(bts)))
        self.__conn.sendall(bts)

    def _recv_string(self):
        btlen = struct.unpack('>Q', recv_exactly(self.__conn, 8))[0]
        return recv_exactly(self.__conn, btlen).decode('UTF-8')

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
            if self.__conn is not None:
                self.__conn.close()
            self.__conn = None
            return

        if not self.ensure_connected():
            return

        assert self.__conn is not None

        try:
            self.__conn.sendall(b'getfullstate\n')
            self.__conn.sendall(struct.pack('>??', self.__skip_dead, self.__skip_archived_groups))
            if not self.__task_group_filter:
                self.__conn.sendall(struct.pack('>I', 0))
            else:
                self.__conn.sendall(struct.pack('>I', len(self.__task_group_filter)))
                for group in self.__task_group_filter:
                    self._send_string(group)
            recvdata = recv_exactly(self.__conn, 8)
        except ConnectionError as e:
            logger.error(f'connection reset {e}')
            logger.error('scheduler connection lost')
            self.__conn = None
            return
        except Exception:
            logger.exception('problems in network operations')
            self.__conn = None
            return
        if len(recvdata) != 8:  # means connection was closed
            logger.error('scheduler connection lost')
            self.__conn = None
            return
        uidatasize = struct.unpack('>Q', recvdata)[0]
        logger.debug(f'fullstate: {uidatasize}B')
        uidatabytes = recv_exactly(self.__conn, uidatasize)
        if len(uidatabytes) != uidatasize:
            logger.error('scheduler connection lost')
            return
        uidata = UiData.deserialize_noasync(uidatabytes)
        # if self.__filter_dead:
        #     uidata = UiData(ui_nodes=uidata.nodes(),
        #                     ui_connections=uidata.connections(),
        #                     ui_tasks={k: v for k, v in uidata.tasks().items() if v['state'] != TaskState.DEAD.value},
        #                     ui_workers=uidata.workers(),
        #                     all_task_groups=uidata.task_groups())
        self.full_update.emit(uidata)

    @Slot(int)
    def get_invocation_metadata(self, task_id: int):
        if not self.ensure_connected():
            return

        assert self.__conn is not None
        try:
            self.__conn.sendall(b'getinvocmeta\n')
            self.__conn.sendall(struct.pack('>Q', task_id))
            rcvsize = struct.unpack('>I', recv_exactly(self.__conn, 4))[0]
            invocmeta = pickle.loads(recv_exactly(self.__conn, rcvsize))
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
        assert self.__conn is not None

        try:
            self.__conn.sendall(b'gettaskattribs\n')
            self.__conn.sendall(struct.pack('>Q', task_id))
            rcvsize = struct.unpack('>Q', recv_exactly(self.__conn, 8))[0]
            attribs = pickle.loads(recv_exactly(self.__conn, rcvsize))
            rcvsize = struct.unpack('>Q', recv_exactly(self.__conn, 8))[0]
            env_attrs = None
            if rcvsize > 0:
                env_attrs = EnvironmentResolverArguments.deserialize(recv_exactly(self.__conn, rcvsize))
        except ConnectionError as e:
            logger.error(f'failed {e}')
        except Exception:
            logger.exception('problems in network operations')
        else:
            self.task_attribs_fetched.emit(task_id, (attribs, env_attrs), data)

    @Slot(int)
    def get_task_invocation_job(self, task_id: int):
        if not self.ensure_connected():
            return
        assert self.__conn is not None

        try:
            self.__conn.sendall(b'gettaskinvoc\n')
            self.__conn.sendall(struct.pack('>Q', task_id))
            rcvsize = struct.unpack('>Q', recv_exactly(self.__conn, 8))[0]
            if rcvsize == 0:
                invoc = InvocationJob([])
            else:
                invoc = InvocationJob.deserialize(recv_exactly(self.__conn, rcvsize))
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

        assert self.__conn is not None
        try:
            self.__conn.sendall(b'getlog\n')
            self.__conn.sendall(struct.pack('>QQQ', task_id, node_id, invocation_id))
            rcvsize = struct.unpack('>I', recv_exactly(self.__conn, 4))[0]
            alllogs = pickle.loads(recv_exactly(self.__conn, rcvsize))
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
        assert self.__conn is not None
        try:
            self.__conn.sendall(b'getnodeinterface\n')
            self.__conn.sendall(struct.pack('>Q', node_id))
            rcvsize = struct.unpack('>I', recv_exactly(self.__conn, 4))[0]
            nodeui: NodeUi = pickle.loads(recv_exactly(self.__conn, rcvsize))
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
        assert self.__conn is not None
        try:
            self.__conn.sendall(b'nodehasparam\n')
            self.__conn.sendall(struct.pack('>Q', node_id))
            self._send_string(param_name)
            good = recv_exactly(self.__conn, 1) == b'\1'
            self.node_has_parameter.emit(node_id, param_name, good, data)
        except ConnectionError as e:
            logger.error(f'failed {e}')
        except Exception:
            logger.exception('problems in network operations')

    @Slot()
    def send_node_parameter_change(self, node_id: int, param: Parameter, data=None):
        if not self.ensure_connected():
            return
        assert self.__conn is not None
        try:
            param_type = param.type()
            param_value = param.unexpanded_value()
            self.__conn.sendall(b'setnodeparam\n')
            param_name_data = param.name().encode('UTF-8')
            self.__conn.sendall(struct.pack('>QII', node_id, param_type.value, len(param_name_data)))
            self.__conn.sendall(param_name_data)
            newval = None
            if param_type == NodeParameterType.FLOAT:
                self.__conn.sendall(struct.pack('>d', param_value))
                if recv_exactly(self.__conn, 1) == b'\1':
                    newval = struct.unpack('>d', recv_exactly(self.__conn, 8))[0]
            elif param_type == NodeParameterType.INT:
                self.__conn.sendall(struct.pack('>q', param_value))
                if recv_exactly(self.__conn, 1) == b'\1':
                    newval = struct.unpack('>q', recv_exactly(self.__conn, 8))[0]
            elif param_type == NodeParameterType.BOOL:
                self.__conn.sendall(struct.pack('>?', param_value))
                if recv_exactly(self.__conn, 1) == b'\1':
                    newval = struct.unpack('>?', recv_exactly(self.__conn, 1))[0]
            elif param_type == NodeParameterType.STRING:
                self._send_string(param_value)
                if recv_exactly(self.__conn, 1) == b'\1':
                    newval = self._recv_string()
            else:
                raise NotImplementedError()
            self.node_parameter_changed.emit(node_id, param, newval, data)
        except ConnectionError as e:
            logger.error(f'failed {e}')
        except Exception:
            logger.exception('problems in network operations')

    @Slot()
    def send_node_parameter_expression_change(self, node_id: int, param: Parameter, data=None):
        if not self.ensure_connected():
            return
        assert self.__conn is not None
        try:
            set_or_unset = param.has_expression()
            self.__conn.sendall(b'setnodeparamexpression\n')
            self.__conn.sendall(struct.pack('>Q?', node_id, set_or_unset))
            self._send_string(param.name())
            if set_or_unset:
                expression = param.expression()
                self._send_string(expression)
            assert recv_exactly(self.__conn, 1) == b'\1'
            self.node_parameter_expression_changed.emit(node_id, param, data)
        except ConnectionError as e:
            logger.error(f'failed {e}')
        except Exception:
            logger.exception('problems in network operations')

    @Slot()
    def apply_node_settings(self, node_id: int, settings_name: str, data):
        if not self.ensure_connected():
            return
        assert self.__conn is not None
        try:
            self.__conn.sendall(b'applynodesettings\n')
            self.__conn.sendall(struct.pack('>Q', node_id))
            self._send_string(settings_name)
            recv_exactly(self.__conn, 1)  # receiving and ignoring result
            self.node_settings_applied.emit(node_id, settings_name, data)
        except ConnectionError as e:
            logger.error(f'failed {e}')
        except Exception:
            logger.exception('problems in network operations')

    @Slot()
    def node_save_custom_settings(self, node_type_name: str, settings_name: str, settings: dict, data):
        if not self.ensure_connected():
            return
        assert self.__conn is not None
        try:
            self.__conn.sendall(b'savecustomnodesettings\n')
            self._send_string(node_type_name)
            self._send_string(settings_name)
            settings_data = pickle.dumps(settings)
            self.__conn.sendall(struct.pack('>Q', len(settings_data)))
            self.__conn.sendall(settings_data)
            recv_exactly(self.__conn, 1)  # ignore result for now
            self.node_custom_settings_saved.emit(node_type_name, settings_name, data)
        except ConnectionError as e:
            logger.error(f'failed {e}')
        except Exception:
            logger.exception('problems in network operations')

    @Slot()
    def node_set_settings_default(self, node_type_name: str, settings_name: Optional[str], data):
        if not self.ensure_connected():
            return
        assert self.__conn is not None
        try:
            self.__conn.sendall(b'setsettingsdefault\n')
            self._send_string(node_type_name)
            self.__conn.sendall(struct.pack('>?', settings_name is not None))
            if settings_name is not None:
                self._send_string(settings_name)
            recv_exactly(self.__conn, 1)  # ignore result for now
            self.node_default_settings_set.emit(node_type_name, settings_name, data)
        except ConnectionError as e:
            logger.error(f'failed {e}')
        except Exception:
            logger.exception('problems in network operations')

    @Slot()
    def get_nodetypes(self):
        if not self.ensure_connected():
            return
        assert self.__conn is not None
        nodetypes: Dict[str, NodeTypeMetadata] = {}
        try:
            metas: List[NodeTypeMetadata] = []
            self.__conn.sendall(b'listnodetypes\n')
            elemcount = struct.unpack('>Q', recv_exactly(self.__conn, 8))[0]
            for i in range(elemcount):
                btlen = struct.unpack('>Q', recv_exactly(self.__conn, 8))[0]
                metas.append(pickle.loads(recv_exactly(self.__conn, btlen)))
            nodetypes = {n.type_name: n for n in metas}
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
        assert self.__conn is not None
        try:
            self.__conn.sendall(b'listnodepresets\n')
            btlen = struct.unpack('>Q', recv_exactly(self.__conn, 8))[0]
            presets: Dict[str, List[str]] = pickle.loads(recv_exactly(self.__conn, btlen))
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
        assert self.__conn is not None
        try:
            self.__conn.sendall(b'getnodepreset\n')
            self._send_string(package)
            self._send_string(preset)
            good = struct.unpack('>?', recv_exactly(self.__conn, 1))[0]
            if not good:
                logger.error(f'requested node preset not found {package}::{preset}')
                return
            btlen = struct.unpack('>Q', recv_exactly(self.__conn, 8))[0]
            snippet: NodeSnippetData = NodeSnippetData.deserialize(recv_exactly(self.__conn, btlen))
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
        assert self.__conn is not None
        try:
            self.__conn.sendall(b'addnode\n')
            self._send_string(node_type)
            self._send_string(node_name)
            node_id = struct.unpack('>Q', recv_exactly(self.__conn, 8))[0]
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
        assert self.__conn is not None
        try:
            self.__conn.sendall(b'removenode\n')
            self.__conn.sendall(struct.pack('>Q', node_id))
            assert recv_exactly(self.__conn, 1) == b'\1'
        except ConnectionError as e:
            logger.error(f'failed {e}')
        except Exception:
            logger.exception('problems in network operations')

    @Slot()
    def wipe_node(self, node_id: int):
        if not self.ensure_connected():
            return
        assert self.__conn is not None
        try:
            self.__conn.sendall(b'wipenode\n')
            self.__conn.sendall(struct.pack('>Q', node_id))
            assert recv_exactly(self.__conn, 1) == b'\1'
        except ConnectionError as e:
            logger.error(f'failed {e}')
        except Exception:
            logger.exception('problems in network operations')

    @Slot()
    def set_node_name(self, node_id: int, node_name: str):
        if not self.ensure_connected():
            return
        assert self.__conn is not None
        try:
            self.__conn.sendall(b'renamenode\n')
            self.__conn.sendall(struct.pack('>Q', node_id))
            self._send_string(node_name)
            _ = self._recv_string()
        except ConnectionError as e:
            logger.error(f'failed {e}')
        except Exception:
            logger.exception('problems in network operations')

    @Slot()
    def duplicate_nodes(self, node_ids: List[int], shift: QPointF):
        if not self.ensure_connected():
            return
        assert self.__conn is not None
        try:
            self.__conn.sendall(b'duplicatenodes\n')
            self.__conn.sendall(struct.pack('>Q', len(node_ids)))
            for node_id in node_ids:
                self.__conn.sendall(struct.pack('>Q', node_id))
            result = recv_exactly(self.__conn, 1)
            if result == b'\0':
                return
            cnt = struct.unpack('>Q', recv_exactly(self.__conn, 8))[0]
            ret = {}
            for i in range(cnt):
                old_id, new_id = struct.unpack('>QQ', recv_exactly(self.__conn, 16))
                assert old_id in node_ids
                ret[old_id] = new_id
            self.nodes_copied.emit(ret, shift)
        except ConnectionError as e:
            logger.error(f'failed {e}')
        except Exception:
            logger.exception('problems in network operations')

    @Slot()
    def change_node_connection(self, connection_id: int, outnode_id: Optional[int] = None, outname: Optional[str] = None, innode_id: Optional[int] = None, inname: Optional[str] = None):
        if not self.ensure_connected():
            return
        assert self.__conn is not None
        try:
            logger.debug(f'{connection_id}, {outnode_id}, {outname}, {innode_id}, {inname}')
            self.__conn.sendall(b'changeconnection\n')
            self.__conn.sendall(struct.pack('>Q??QQ', connection_id, outnode_id is not None, innode_id is not None, outnode_id or 0, innode_id or 0))
            if outnode_id is not None:
                self._send_string(outname)
            if innode_id is not None:
                self._send_string(inname)
            assert recv_exactly(self.__conn, 1) == b'\1'
        except ConnectionError as e:
            logger.error(f'failed {e}')
        except Exception:
            logger.exception('problems in network operations')

    @Slot()
    def add_node_connection(self, outnode_id: int, outname: str, innode_id: int, inname: str):
        if not self.ensure_connected():
            return
        assert self.__conn is not None
        try:
            self.__conn.sendall(b'addconnection\n')
            self.__conn.sendall(struct.pack('>QQ', outnode_id, innode_id))
            self._send_string(outname)
            self._send_string(inname)
            new_id = struct.unpack('>Q', recv_exactly(self.__conn, 8))[0]
        except ConnectionError as e:
            logger.error(f'failed {e}')
        except Exception:
            logger.exception('problems in network operations')

    @Slot()
    def remove_node_connection(self, connection_id: int):
        if not self.ensure_connected():
            return
        assert self.__conn is not None
        try:
            self.__conn.sendall(b'removeconnection\n')
            self.__conn.sendall(struct.pack('>Q', connection_id))
            assert recv_exactly(self.__conn, 1) == b'\1'
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
        assert self.__conn is not None

        try:
            if isinstance(task_ids_or_group, str):
                self.__conn.sendall(b'tpausegrp\n')
                self.__conn.sendall(struct.pack('>?', paused))
                self._send_string(task_ids_or_group)
            else:
                if isinstance(task_ids_or_group, int):
                    task_ids_or_group = [task_ids_or_group]
                numtasks = len(task_ids_or_group)
                if numtasks == 0:
                    return
                self.__conn.sendall(b'tpauselst\n')
                self.__conn.sendall(struct.pack('>Q?Q', numtasks, paused, task_ids_or_group[0]))
                if numtasks > 1:
                    self.__conn.sendall(struct.pack('>' + 'Q' * (numtasks-1), *task_ids_or_group[1:]))
            assert recv_exactly(self.__conn, 1) == b'\1'
        except ConnectionError as e:
            logger.error(f'failed {e}')
        except Exception:
            logger.exception('problems in network operations')

    @Slot()
    def set_task_group_archived_state(self, task_group_name: str, state: TaskGroupArchivedState):
        if not self.ensure_connected():
            return
        assert self.__conn is not None

        try:
            self.__conn.sendall(b'tarchivegrp\n')
            self._send_string(task_group_name)
            self.__conn.sendall(struct.pack('>I', state.value))
            assert recv_exactly(self.__conn, 1) == b'\1'
        except ConnectionError as e:
            logger.error(f'failed {e}')
        except Exception:
            logger.exception('problems in network operations')

    @Slot()
    def set_task_node(self, task_id: int, node_id: int):
        if not self.ensure_connected():
            return
        assert self.__conn is not None
        try:
            self.__conn.sendall(b'tsetnode\n')
            self.__conn.sendall(struct.pack('>QQ', task_id, node_id))
            assert recv_exactly(self.__conn, 1) == b'\1'
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
        assert self.__conn is not None
        try:
            self.__conn.sendall(b'tcstate\n')
            self.__conn.sendall(struct.pack('>QIQ', numtasks, state.value, task_ids[0]))
            if numtasks > 1:
                self.__conn.sendall(struct.pack('>' + 'Q' * (numtasks - 1), *task_ids[1:]))
            assert recv_exactly(self.__conn, 1) == b'\1'
        except ConnectionError as e:
            logger.error(f'failed {e}')
        except Exception:
            logger.exception('problems in network operations')

    @Slot()
    def set_task_name(self, task_id: int, name: str):
        if not self.ensure_connected():
            return
        assert self.__conn is not None
        try:
            self.__conn.sendall(b'tsetname\n')
            self.__conn.sendall(struct.pack('>Q', task_id))
            self._send_string(name)
            assert recv_exactly(self.__conn, 1) == b'\1'
        except ConnectionError as e:
            logger.error(f'failed {e}')
        except Exception:
            logger.exception('problems in network operations')

    @Slot()
    def set_task_groups(self, task_id: int, groups: set):
        if not self.ensure_connected():
            return
        assert self.__conn is not None
        try:
            self.__conn.sendall(b'tsetgroups\n')
            self.__conn.sendall(struct.pack('>QQ', task_id, len(groups)))
            for group in groups:
                self._send_string(group)
            assert recv_exactly(self.__conn, 1) == b'\1'
        except ConnectionError as e:
            logger.error(f'failed {e}')
        except Exception:
            logger.exception('problems in network operations')

    @Slot()
    def update_task_attributes(self, task_id: int, attribs_to_set: dict, attribs_to_delete: set):
        if not self.ensure_connected():
            return
        assert self.__conn is not None
        try:
            self.__conn.sendall(b'tupdateattribs\n')
            data_bytes = pickle.dumps(attribs_to_set)
            self.__conn.sendall(struct.pack('>QQQ', task_id, len(data_bytes), len(attribs_to_delete)))
            self.__conn.sendall(data_bytes)
            for attr in attribs_to_delete:
                self._send_string(attr)
            assert recv_exactly(self.__conn, 1) == b'\1'
        except ConnectionError as e:
            logger.error(f'failed {e}')
        except Exception:
            logger.exception('problems in network operations')

    @Slot()
    def cancel_task(self, task_id: int):
        if not self.ensure_connected():
            return
        assert self.__conn is not None
        try:
            self.__conn.sendall(b'tcancel\n')
            self.__conn.sendall(struct.pack('>Q', task_id))
            assert recv_exactly(self.__conn, 1) == b'\1'
        except ConnectionError as e:
            logger.error(f'failed {e}')
        except Exception:
            logger.exception('problems in network operations')

    @Slot(object)
    def cancel_task_for_worker(self, worker_id: int):
        if not self.ensure_connected():
            return
        assert self.__conn is not None
        try:
            self.__conn.sendall(b'workertaskcancel\n')
            self.__conn.sendall(struct.pack('>Q', worker_id))
            assert recv_exactly(self.__conn, 1) == b'\1'
        except ConnectionError as e:
            logger.error(f'failed {e}')
        except Exception:
            logger.exception('problems in network operations')

    @Slot()
    def add_task(self, new_task: NewTask):
        if not self.ensure_connected():
            return
        assert self.__conn is not None
        data = new_task.serialize()
        try:
            self.__conn.sendall(b'addtask\n')
            self.__conn.sendall(struct.pack('>Q', len(data)))
            self.__conn.sendall(data)
            _ = recv_exactly(self.__conn, 4)  # reply that we don't care about for now
        except ConnectionError as e:
            logger.error(f'failed {e}')
        except Exception:
            logger.exception('problem in network operations')

    @Slot()
    def set_environment_resolver_arguments(self, task_id: int, env_args: Optional[EnvironmentResolverArguments]):
        if not self.ensure_connected():
            return
        assert self.__conn is not None

        try:
            self.__conn.sendall(b'settaskenvresolverargs\n')
            self.__conn.sendall(struct.pack('>Q', task_id))
            if env_args is None:
                self.__conn.sendall(struct.pack('>Q', 0))
            else:
                data = env_args.serialize()
                self.__conn.sendall(struct.pack('>Q', len(data)))
                self.__conn.sendall(data)
            assert recv_exactly(self.__conn, 1) == b'\1'
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
        assert self.__conn is not None

        try:
            self.__conn.sendall(b'setworkergroups\n')
            self.__conn.sendall(struct.pack('>QQ', whwid, len(groups)))
            for group in groups:
                self._send_string(group)
            assert recv_exactly(self.__conn, 1) == b'\1'
        except ConnectionError as e:
            logger.error(f'failed {e}')
        except Exception:
            logger.exception('problem in network operations')
