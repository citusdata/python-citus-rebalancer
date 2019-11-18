from .classes import *


def get_node_by_name(name, nodes):
    for node in nodes:
        if node.name == name:
            return node


def get_data_from_citus_cluster(connection):
    formation_query = '''SELECT sum(result::bigint)::bigint as formation_size
    FROM run_command_on_workers($cmd$SELECT pg_database_size('citus')::bigint;$cmd$)
    '''

    nodes_query = '''SELECT nodename,result::bigint as node_size
    FROM run_command_on_workers($cmd$SELECT pg_database_size('citus')::bigint;$cmd$)
    ORDER BY node_size DESC;'''

    shard_group_query = '''
    WITH shard_sizes AS(
        SELECT shardid, result::bigint size FROM
        (SELECT (run_command_on_shards(logicalrelid::text,$cmd$SELECT pg_total_relation_size('%s')$cmd$)).*
        FROM pg_dist_partition pp where pp.partmethod='h')a)
    SELECT nodename, shard_group, group_size, colocationid
    FROM
    (
       SELECT nodename,array_agg(ps.shardid) shard_group ,sum(size) group_size, colocationid
       from shard_sizes ss,pg_dist_shard ps, pg_dist_shard_placement psp,
       pg_dist_partition pp
       WHERE (ss.shardid=ps.shardid AND pp.logicalrelid=ps.logicalrelid AND psp.shardid=ps.shardid AND pp.partmethod='h')
    GROUP BY shardmaxvalue,shardminvalue,nodename,colocationid
    )a
    ORDER BY nodename, group_size ASC;
    '''

    cursor = connection.cursor()

    cursor.execute(formation_query)
    formation_size = cursor.fetchone()[0]

    cursor.execute(nodes_query)
    nodes_data = cursor.fetchall()

    cursor.execute(shard_group_query)
    shard_data = cursor.fetchall()

    return formation_size, nodes_data, shard_data


def init_formation(formation_size, nodes_data, shard_data, delta):
    formation = Formation(formation_size, delta, len(nodes_data))
    nodes = []

    for node in nodes_data:
        nodes.append(Node(node[0], node[1], formation))

    for row in shard_data:
        node = get_node_by_name(row[0], nodes)
        shard_group = ShardGroup(row[1],row[2], node)
        node.add_shard_group(shard_group)

    formation.set_nodes(nodes)
    formation.set_big_nodes()
    formation.set_small_nodes()

    return formation


def find_necessary_moves(formation):
    moved = []

    while formation.big_nodes and formation.small_nodes:
        big_node= formation.big_nodes[0]

        smaller_nodes_copy = []

        for small_node in formation.small_nodes:
            for group in big_node.shard_groups:
                if not group.is_moveable_to_node(small_node):
                    continue

                small_node.set_size(small_node.size + group.size)
                big_node.set_size(big_node.size - group.size)
                group.set_to_node(small_node)
                moved.append(group)
        formation.set_big_nodes()
        formation.set_small_nodes()

    return formation, moved


def get_master_mode_shard_commands(moves):
    commands = []

    for group in moves:
        commands.append("SELECT master_move_shard_placement(%d,'%s', 5432, '%s', 5432);"
                        % (group.shards[0], group.node.name, group.to_node.name))

    return commands
