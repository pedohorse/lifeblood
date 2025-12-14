import os
import argparse
import sys
import json
from lifeblood.enums import SpawnStatus, NodeParameterType
from lifeblood.environment_resolver import EnvironmentResolverArguments
from lifeblood.node_parameters import Parameter
from lifeblood.scheduler_ui_protocol import UIProtocolSocketClient
from lifeblood.taskspawn import NewTask


DEFAULT_ADDRESS = '127.0.0.1:1385'


def main():
    parser = argparse.ArgumentParser('lifeblood_client', description='query/control lifeblood scheduler')

    parser.add_argument('--address', help='scheduler address in format address:port.'
                                          'if not provided - LIFEBLOOD_RUNTIME_SCHEDULER_ADDR env var will be checked.'
                                          f'as last fallback {DEFAULT_ADDRESS} will be checked')

    subparsers = parser.add_subparsers(title='command', required=True, dest='command')

    #
    # query command
    query_parser = subparsers.add_parser('query')
    query_parser.add_argument('--json', action='store_true', help='print raw data, no extra text')
    query_target_subparsers = query_parser.add_subparsers(title='target', dest='query_target')

    #
    # query workers
    query_workers_parser = query_target_subparsers.add_parser('workers')

    #
    # query tasks
    query_tasks_parser = query_target_subparsers.add_parser('tasks')
    query_tasks_parser.add_argument('--task-ids', dest='query_task_ids', nargs='*', default=None)

    #
    # create command
    create_parser = subparsers.add_parser('create')
    create_subparsers = create_parser.add_subparsers(title='create_entity', required=True, dest='create_entity')

    #
    # create task
    create_task_parser = create_subparsers.add_parser('task')
    create_task_parser.add_argument('--node-id', help='node id on which to create new task', dest='task_node_id')
    create_task_parser.add_argument('--node-name', help='node name on which to create new task.'
                                                               ' there can be many nodes with same name, first node found'
                                                               ' will be used', dest='task_node_name')
    create_task_parser.add_argument('--name', help='task name', dest='task_name', default='new_task')
    create_task_parser.add_argument('--attributes', nargs='*', dest='task_attributes', help='list of <name>=<value> of attributes to set', default=[])
    create_task_parser.add_argument('--env-resolver', default='StandardEnvironmentResolver', dest='task_env_resolver')
    create_task_parser.add_argument('--env-resolver-arguments', nargs='*', dest='task_env_resolver_arguments', default=[])

    #
    # create node
    create_node_parser = create_subparsers.add_parser('node')
    create_node_parser.add_argument('node_type')
    create_node_parser.add_argument('--name', help='node name', dest='node_name', default='new_node')
    create_node_parser.add_argument('--parameters', nargs='*', dest='node_parameters', default=[],
                                    help='list of <name>=<value> of parameters to set on the node',)

    args = parser.parse_args()

    address_str = args.address
    if not address_str:
        address_str = os.environ.get('LIFEBLOOD_RUNTIME_SCHEDULER_ADDR')
    if not address_str:
        address_str = DEFAULT_ADDRESS

    if ':' not in address_str:
        raise RuntimeError('malformed address provided')
    host, port_str = address_str.split(':', 1)
    port = int(port_str)

    if args.command == 'query':
        handle_query(args, host, port)
    elif args.command == 'create':
        handle_create(args, host, port)
    else:
        raise NotImplementedError(f'command "{args.command}" is not implemented')


def handle_query(args, host: str, port: int):
    if args.query_target == 'workers':
        with UIProtocolSocketClient(host, port) as client:
            worker_states = client.get_ui_workers_state()
        if args.json:
            print(json.dumps({wid: wdata.state.value for wid, wdata in worker_states.workers.items()}), end='')
        else:
            for wid, wdata in worker_states.workers.items():
                print(f'Worker({wid}) in state {wdata.state}')

    elif args.query_target == 'tasks':
        if not args.query_task_ids:
            return

        task_states = []
        with UIProtocolSocketClient(host, port) as client:
            for task_id in args.query_task_ids:
                attribs, _ = client.get_task_attribs(int(task_id))
                task_states.append(
                    (
                        task_id,
                        client.get_task_state(int(task_id)),
                        attribs,
                    )
                )
        if args.json:
            print(json.dumps({task_id: {
                'state': state.value,
                'attributes': attributes,
            } for task_id, state, attributes in task_states}), end='')
        else:
            for task_id, state in task_states:
                print(f'Task({task_id}) in state {state}')
    else:
        raise NotImplementedError(f'unknown query target "{args.query_target}"')


def handle_create(args, host: str, port: int):
    if args.create_entity == 'task':
        handle_create_task(args, host, port)
    elif args.create_entity == 'node':
        handle_create_node(args, host, port)
    else:
        raise NotImplementedError(f'unknown create entity "{args.create_entity}"')


def _parse_val(val: str):
    if val in ('True', 'true'):
        return True
    if val in ('False', 'false'):
        return True
    try:
        return float(val)
    except ValueError:
        pass
    try:
        return int(val)
    except ValueError:
        pass
    # special case if a string is just in first and last quotes
    if val.count('"') == 2 and val[0] == '"' == val[-1]:
        return val[1:-1]
    # everything else treat as a string
    # probably should rethink this arbitrary logic
    return val


def handle_create_task(args, host: str, port: int):
    task_name = args.task_name
    task_node_id = args.task_node_id
    task_node_name = args.task_node_name
    if task_node_id is None:
        if task_node_name is None:
            raise ValueError('either --node-id or --node-name must be provided')
        else:
            raise NotImplementedError('TBD')
    raw_attributes = args.task_attributes or []
    resolver_name = args.task_env_resolver
    raw_resolver_args = args.task_env_resolver_arguments or []

    attributes = {}
    for part in raw_attributes:
        if '=' not in part:
            raise ValueError('--attributes values must contain = sign separating name from value')
        aname, aval = part.split('=', 1)
        aval = _parse_val(aval)
        attributes[aname] = aval
    resolver_args = {}
    for part in raw_resolver_args:
        if '=' not in part:
            raise ValueError('--env-resolver-arguments values must contain = sign separating name from value')
        aname, aval = part.split('=', 1)
        aval = _parse_val(aval)
        resolver_args[aname] = aval

    with UIProtocolSocketClient(host, port) as client:
        status, new_task_id = client.add_task(NewTask(
            task_name,
            task_node_id,
            EnvironmentResolverArguments(
                resolver_name,
                resolver_args,
            ),
            attributes
        ))
    if status == SpawnStatus.SUCCEEDED:
        print(f'created task {new_task_id}', file=sys.stderr)
    else:
        print('failed to create task!', file=sys.stderr)
    # output to stdout
    print(new_task_id, end='')


def handle_create_node(args, host: str, port: int):
    node_type = args.node_type
    node_name = args.node_name

    raw_parameters = args.node_parameters
    parameters = []
    for part in raw_parameters:
        if '=' not in part:
            raise ValueError('--attributes values must contain = sign separating name from value')
        pname, pval = part.split('=', 1)
        pval = _parse_val(pval)
        if isinstance(pval, float):
            ptype = NodeParameterType.FLOAT
        elif isinstance(pval, int):
            ptype = NodeParameterType.INT
        elif isinstance(pval, bool):
            ptype = NodeParameterType.BOOL
        elif isinstance(pval, str):
            ptype = NodeParameterType.STRING
        else:
            raise NotImplementedError(f'unknown param type for value {repr(pval)}')

        parameters.append(Parameter(pname, None, ptype, pval))

    with UIProtocolSocketClient(host, port) as client:
        node_id = client.add_node(node_type, node_name)
        client.set_node_params(node_id, parameters)

    # output node id to stdout
    print(node_id, end='')


if __name__ == '__main__':
    main()

