"""
Function src.discover.table_links.create_db.create_db()
"""
import json
import logging
import pickle
import rustworkx as rx  # pip install rustworkx

logger = logging.getLogger(__name__)


def create_db(
    input_data_filepath: str, max_path_len: int, output_filepath: str
) -> None:
    """Disovers all pathes between tables and writes this information into a .JSON file

    Notes: builds a graph in which every table/column pair is a node,
            and then traverses this graph to find joining paths
    """
    logger.info("Loading input data from [%s]", input_data_filepath)
    with open(input_data_filepath, "r", encoding="utf-8") as file:
        col_pairs = json.load(file)

    logger.info("Building graph")
    gph = rx.PyGraph()
    node_name_to_idx: dict[str, int] = {}
    node_idx_to_name: dict[int, str] = {}
    edge_name_to_idx: dict[str, int] = {}
    for (t1, c1), (t2, c2) in col_pairs:
        # add nodes to graph if they aren't there
        for node_t, node_c in ((t1, c1), (t2, c2)):
            node_name = f"{node_t}::{node_c}"
            if node_name not in node_name_to_idx:
                assigned_idx = gph.add_node(node_name)
                node_name_to_idx[node_name] = assigned_idx
                node_idx_to_name[assigned_idx] = node_name
        # add edge if it isn't there
        edge_name = f"{t1}::{c1} -> {t2}::{c2}"
        if edge_name not in edge_name_to_idx:
            edge_name_to_idx[edge_name] = gph.add_edge(
                node_name_to_idx[f"{t1}::{c1}"], node_name_to_idx[f"{t2}::{c2}"], None
            )

    logger.info("Generating paths between nodes")
    all_pairs_all_simple_paths = rx.all_pairs_all_simple_paths(
        gph,
        cutoff=max_path_len,
    )
    all_node_paths: dict[tuple, set] = {}
    for src_node_idx, many_paths in all_pairs_all_simple_paths.items():
        src_table_name = node_idx_to_name[src_node_idx].split("::")[0]
        for dest_node_idx, paths in many_paths.items():
            dest_table_name = node_idx_to_name[dest_node_idx].split("::")[0]
            if (src_table_name, dest_table_name) not in all_node_paths:
                all_node_paths[(src_table_name, dest_table_name)] = set()
            for path in paths:
                path_str = node_idx_to_name[path[0]]
                for idx in range(1, len(path) - 1):
                    src_colname = node_idx_to_name[path[idx - 1]].split("::")[1]
                    dst_colname = node_idx_to_name[path[idx]].split("::")[1]
                    if src_colname != dst_colname:
                        path_str += f" -> {node_idx_to_name[idx]}"
                path_str += f" -> {node_idx_to_name[path[-1]]}"
                all_node_paths[(src_table_name, dest_table_name)].add(path_str)

    logger.info("Writing output to [%s]", output_filepath)
    with open(output_filepath, "wb") as file:
        pickle.dump(all_node_paths, file, pickle.HIGHEST_PROTOCOL)
