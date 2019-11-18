import click
import re

import psycopg2

from .base import (get_data_from_citus_cluster, init_formation,
                   find_necessary_moves)


def sizeof_fmt(num, suffix='B'):
    for unit in ['','Ki','Mi','Gi','Ti','Pi','Ei','Zi']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)


@click.command()
@click.option('--host', required=True, type=str, help='connection string to your citus coordinator')
@click.option('--delta', required=False, type=int, default=5,
              help='Maximum delta in percent you want between the size of your node, default=5')
def find_ideal_rebalance(host, delta):
    conn = psycopg2.connect(host)

    # get data for the formation from citus cluster
    formation_size, nodes_data, shards_data = get_data_from_citus_cluster(conn)
    formation = init_formation(formation_size, nodes_data, shards_data, delta)

    formation, moved = find_necessary_moves(formation)

    print('Here are the future size of your nodes:\n')
    for node in formation.nodes:
        print('Node: %s will go from %s to %s' % (node.name, sizeof_fmt(node.original_size), sizeof_fmt(node.size)))

    print('Here are the commands to run')
    for group in moved:
        print("SELECT master_move_shard_placement(%d,'%s', 5432, '%s', 5432);" % (group.shards[0], group.node.name, group.to_node.name))


if __name__ == '__main__':
    find_ideal_rebalance()
