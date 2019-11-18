import click
import re

import psycopg2

from .base import (get_data_from_citus_cluster, init_formation,
                   find_necessary_moves, get_master_mode_shard_commands)


def sizeof_fmt(num, suffix='B'):
    num = float(num)
    for unit in ['','Ki','Mi','Gi','Ti','Pi','Ei','Zi']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= float(1024.0)
    return "%.1f%s%s" % (num, 'Yi', suffix)


@click.command()
@click.option('--host', required=True, type=str, help='connection string to your citus coordinator')
@click.option('--delta', required=False, type=int, default=5,
              help='Maximum delta in percent you want between the size of your node, default=5')
@click.option('file_path', '--file', required=False, type=click.Path(),
              help='Path to the file you want to write the rebalance commands to. A good example would be rebalance.sql, you can then execute that file on your coordinator')
def find_ideal_rebalance(host, delta, file_path):
    conn = psycopg2.connect(host)

    # get data for the formation from citus cluster
    formation_size, nodes_data, shards_data = get_data_from_citus_cluster(conn)
    formation = init_formation(formation_size, nodes_data, shards_data, delta)

    formation, moved = find_necessary_moves(formation)

    if not moved:
        print('Your cluster is already balanced')
        return None

    commands = get_master_mode_shard_commands(moved)

    print('Here are the future size of your nodes:\n')
    for node in formation.nodes:
        print('Node: %s will go from %s to %s' % (node.name, sizeof_fmt(node.original_size), sizeof_fmt(node.size)))

    print('Here are the commands to run')
    for command in commands:
        print(command)

    if file_path:
        f = open(file_path, "w")
        for command in commands:
            f.write('%s\n' % command)
        f.close()
        print('The commands to run have been written to %s' % file_path)


if __name__ == '__main__':
    find_ideal_rebalance()
