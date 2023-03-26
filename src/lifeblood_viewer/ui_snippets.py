from lifeblood.snippets import NodeSnippetData
from .graphics_items import Node

from typing import Dict, Iterable, Optional


class UiNodeSnippetData(NodeSnippetData):
    """
    class containing enough information to reproduce a certain snippet of nodes, with parameter values and connections ofc
    """
    @classmethod
    def from_viewer_nodes(cls, nodes: Iterable[Node], preset_label: Optional[str] = None, include_dangling_connections=False):
        clipnodes = []
        clipconns = []
        old_to_tmp: Dict[int, int] = {}
        all_clip_nodes = set()
        # tmpid = 0
        for node in nodes:
            if not isinstance(node, Node):
                continue
            all_clip_nodes.add(node)
            params: Dict[str, "UiNodeSnippetData.ParamData"] = {}
            old_to_tmp[node.get_id()] = node.get_session_id()
            nodedata = UiNodeSnippetData.NodeData(node.get_session_id(),
                                                  node.node_type(),
                                                  node.node_name(),
                                                  params,
                                                  node.pos().toTuple())

            # tmpid += 1

            nodeui = node.get_nodeui()
            if nodeui is not None:
                for param in nodeui.parameters():
                    param_data = UiNodeSnippetData.ParamData(param.name(),
                                                             param.type(),
                                                             param.unexpanded_value(),
                                                             param.expression())
                    params[param.name()] = param_data

            clipnodes.append(nodedata)

        if len(all_clip_nodes) == 0:
            return

        # now connections
        for node in all_clip_nodes:
            for out_name in node.output_names():
                for conn in node.output_connections(out_name):
                    other_node, other_name = conn.input()
                    if not include_dangling_connections:
                        if other_node not in all_clip_nodes:
                            continue
                        else:
                            assert conn.output()[0].get_id() in old_to_tmp
                            assert conn.input()[0].get_id() in old_to_tmp
                    clipconns.append(UiNodeSnippetData.ConnData(old_to_tmp.get(conn.output()[0].get_id(), conn.output()[0].get_session_id()), conn.output()[1],
                                                                old_to_tmp.get(conn.input()[0].get_id(), conn.input()[0].get_session_id()), conn.input()[1]))
            # extra case for dangling inputs
            if include_dangling_connections:
                for in_name in node.input_names():
                    for conn in node.input_connections(in_name):
                        other_node, other_name = conn.output()
                        if other_node in all_clip_nodes:  #  we don't care about nodes in the snippet - that is all covered by prev for
                            continue
                        assert other_node.get_id() not in old_to_tmp
                        clipconns.append(UiNodeSnippetData.ConnData(other_node.get_session_id(), conn.output()[1],
                                                                    old_to_tmp.get(conn.input()[0].get_id(), conn.input()[0].get_session_id()), conn.input()[1]))

        return UiNodeSnippetData(clipnodes, clipconns, preset_label)

    @classmethod
    def from_node_snippet_data(cls, node_snippet_data: NodeSnippetData) -> "UiNodeSnippetData":
        data = UiNodeSnippetData([], [])
        data.__dict__.update(node_snippet_data.__dict__)
        return data

    def __init__(self, nodes_data: Iterable[NodeSnippetData.NodeData], connections_data: Iterable[NodeSnippetData.ConnData], preset_label: Optional[str] = None):
        super(UiNodeSnippetData, self).__init__(nodes_data, connections_data, preset_label)
