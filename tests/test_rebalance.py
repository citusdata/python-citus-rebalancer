import pytest

from citus_rebalancer.base import *


def test_init_formation():
    nodes_data = [('node-1-amazonaws.com', 1000000000), ('node-2-amazonaws.com', 2000000000),
                  ('node-3-amazonaws.com', 3000000000), ('node-4-amazonaws.com', 4000000000)]


    shard_data = [
        ('node-1-amazonaws.com', [1, 2], 200000000, 1),
        ('node-1-amazonaws.com', [3, 4], 50000000, 1),
        ('node-1-amazonaws.com', [5, 6], 50000000, 1),
        ('node-1-amazonaws.com', [5, 6], 100000000, 1),
        ('node-1-amazonaws.com', [7, 8], 300000000, 1),
        ('node-1-amazonaws.com', [9, 10], 100000000, 1),
        ('node-1-amazonaws.com', [11, 12], 150000000, 1),
        ('node-1-amazonaws.com', [13, 14], 50000000, 1),

        ('node-2-amazonaws.com', [15, 16], 450000000, 1),
        ('node-2-amazonaws.com', [17, 18], 50000000, 1),
        ('node-2-amazonaws.com', [19, 20], 20000000, 1),
        ('node-2-amazonaws.com', [21, 22], 80000000, 1),
        ('node-2-amazonaws.com', [23, 24], 400000000, 1),
        ('node-2-amazonaws.com', [25, 26], 500000000, 1),
        ('node-2-amazonaws.com', [27, 28], 250000000, 1),
        ('node-2-amazonaws.com', [29, 30], 250000000, 1),

        ('node-3-amazonaws.com', [31, 32], 1000000000, 1),
        ('node-3-amazonaws.com', [33, 34], 600000000, 1),
        ('node-3-amazonaws.com', [35, 36], 200000000, 1),
        ('node-3-amazonaws.com', [36, 37], 600000000, 1),
        ('node-3-amazonaws.com', [38, 39], 80000000, 1),
        ('node-3-amazonaws.com', [40, 41], 20000000 , 1),
        ('node-3-amazonaws.com', [42, 43], 250000000, 1),
        ('node-3-amazonaws.com', [44, 45], 250000000, 1),

        ('node-4-amazonaws.com', [46, 47], 900000000, 1),
        ('node-4-amazonaws.com', [48, 49], 100000000, 1),
        ('node-4-amazonaws.com', [50, 51], 40000000, 1),
        ('node-4-amazonaws.com', [52, 53], 160000000, 1),
        ('node-4-amazonaws.com', [54, 55], 800000000, 1),
        ('node-4-amazonaws.com', [56, 57], 1000000000, 1),
        ('node-4-amazonaws.com', [58, 59], 500000000, 1),
        ('node-4-amazonaws.com', [60, 61], 500000000, 1),
    ]

    formation = init_formation(10000000000, nodes_data, shard_data, 3)

    assert formation.ideal_node_size == 2500000000.0
    assert formation.min_node_size == 2200000000.0
    assert formation.max_node_size == 2800000000.0

    nodes = formation.nodes

    assert len(nodes) == 4

    for node in nodes:
        if node.name in ('node-1-amazonaws.com', 'node-2-amazonaws.com'):
            assert node.is_too_small() is True
            assert node.is_too_big() is False
            assert node in formation.small_nodes

        if node.name in ('node-3-amazonaws.com', 'node-4-amazonaws.com'):
            assert node.is_too_small() is False
            assert node.is_too_big() is True
            assert node in formation.big_nodes


def test_find_necessary_moves_balanced_cluster():
    # Nodes are same size
    nodes_data = [('node-1.amazonaws.com', 2247266975), ('node-2.amazonaws.com', 2227597983),
                  ('node-3.amazonaws.com', 2247266975), ('node-4.amazonaws.com', 2227597983)]

    shard_data = [
        ('node-1.amazonaws.com', [102090, 102058, 102026], 133349376, 1),
        ('node-1.amazonaws.com', [102096, 102064, 102032], 134242304, 1),
        ('node-1.amazonaws.com', [102008, 102040, 102072], 135888896, 1),
        ('node-1.amazonaws.com', [102038, 102102, 102070], 136585216, 1),
        ('node-1.amazonaws.com', [102028, 102060, 102092], 136749056, 1),
        ('node-1.amazonaws.com', [102010, 102042, 102074], 137322496, 1),
        ('node-1.amazonaws.com', [102022, 102054, 102086], 137846784, 1),
        ('node-1.amazonaws.com', [102044, 102012, 102076], 137953280, 1),
        ('node-1.amazonaws.com', [102078, 102046, 102014], 138739712, 1),
        ('node-1.amazonaws.com', [102056, 102024, 102088], 138813440, 1),
        ('node-1.amazonaws.com', [102020, 102084, 102052], 138928128, 1),
        ('node-1.amazonaws.com', [102100, 102036, 102068], 139042816, 1),
        ('node-1.amazonaws.com', [102080, 102016, 102048], 139296768, 1),
        ('node-1.amazonaws.com', [102034, 102098, 102066], 141148160, 1),
        ('node-1.amazonaws.com', [102050, 102018, 102082], 141631488, 1),
        ('node-1.amazonaws.com', [102094, 102030, 102062], 141877248, 1),
        ('node-2.amazonaws.com', [102089, 102057, 102025], 135815168, 1),
        ('node-2.amazonaws.com', [102021, 102053, 102085], 136790016, 1),
        ('node-2.amazonaws.com', [102073, 102041, 102009], 136847360, 1),
        ('node-2.amazonaws.com', [102099, 102067, 102035], 137781248, 1),
        ('node-2.amazonaws.com', [102071, 102103, 102039], 138190848, 1),
        ('node-2.amazonaws.com', [102061, 102029, 102093], 138321920, 1),
        ('node-2.amazonaws.com', [102047, 102015, 102079], 138600448, 1),
        ('node-2.amazonaws.com', [102019, 102083, 102051], 138805248, 1),
        ('node-2.amazonaws.com', [102037, 102101, 102069], 139034624, 1),
        ('node-2.amazonaws.com', [102063, 102095, 102031], 139313152, 1),
        ('node-2.amazonaws.com', [102091, 102059, 102027], 140378112, 1),
        ('node-2.amazonaws.com', [102081, 102049, 102017], 141074432, 1),
        ('node-2.amazonaws.com', [102045, 102077, 102013], 141221888, 1),
        ('node-2.amazonaws.com', [102065, 102097, 102033], 141377536, 1),
        ('node-2.amazonaws.com', [102023, 102055, 102087], 141615104, 1),
        ('node-2.amazonaws.com', [102011, 102043, 102075], 143925248, 1),
        ('node-3.amazonaws.com', [102090, 102058, 102026], 133349376, 1),
        ('node-3.amazonaws.com', [102096, 102064, 102032], 134242304, 1),
        ('node-3.amazonaws.com', [102008, 102040, 102072], 135888896, 1),
        ('node-3.amazonaws.com', [102038, 102102, 102070], 136585216, 1),
        ('node-3.amazonaws.com', [102028, 102060, 102092], 136749056, 1),
        ('node-3.amazonaws.com', [102010, 102042, 102074], 137322496, 1),
        ('node-3.amazonaws.com', [102022, 102054, 102086], 137846784, 1),
        ('node-3.amazonaws.com', [102044, 102012, 102076], 137953280, 1),
        ('node-3.amazonaws.com', [102078, 102046, 102014], 138739712, 1),
        ('node-3.amazonaws.com', [102056, 102024, 102088], 138813440, 1),
        ('node-3.amazonaws.com', [102020, 102084, 102052], 138928128, 1),
        ('node-3.amazonaws.com', [102100, 102036, 102068], 139042816, 1),
        ('node-3.amazonaws.com', [102080, 102016, 102048], 139296768, 1),
        ('node-3.amazonaws.com', [102034, 102098, 102066], 141148160, 1),
        ('node-3.amazonaws.com', [102050, 102018, 102082], 141631488, 1),
        ('node-3.amazonaws.com', [102094, 102030, 102062], 141877248, 1),
        ('node-4.amazonaws.com', [102089, 102057, 102025], 135815168, 1),
        ('node-4.amazonaws.com', [102021, 102053, 102085], 136790016, 1),
        ('node-4.amazonaws.com', [102073, 102041, 102009], 136847360, 1),
        ('node-4.amazonaws.com', [102099, 102067, 102035], 137781248, 1),
        ('node-4.amazonaws.com', [102071, 102103, 102039], 138190848, 1),
        ('node-4.amazonaws.com', [102061, 102029, 102093], 138321920, 1),
        ('node-4.amazonaws.com', [102047, 102015, 102079], 138600448, 1),
        ('node-4.amazonaws.com', [102019, 102083, 102051], 138805248, 1),
        ('node-4.amazonaws.com', [102037, 102101, 102069], 139034624, 1),
        ('node-4.amazonaws.com', [102063, 102095, 102031], 139313152, 1),
        ('node-4.amazonaws.com', [102091, 102059, 102027], 140378112, 1),
        ('node-4.amazonaws.com', [102081, 102049, 102017], 141074432, 1),
        ('node-4.amazonaws.com', [102045, 102077, 102013], 141221888, 1),
        ('node-4.amazonaws.com', [102065, 102097, 102033], 141377536, 1),
        ('node-4.amazonaws.com', [102023, 102055, 102087], 141615104, 1),
        ('node-4.amazonaws.com', [102011, 102043, 102075], 143925248, 1)]

    formation = init_formation(8949729916, nodes_data, shard_data, 10)
    formation, moved = find_necessary_moves(formation)

    for node in formation.nodes:
        assert node.original_size == node.size

    assert len(moved) == 0


def test_necessary_moves_empty_nodes():
    nodes_data = [('node-1-amazonaws.com', 4000000000), ('node-2-amazonaws.com', 2000000000),
                  ('node-3-amazonaws.com', 3000000000), ('node-4-amazonaws.com', 0)]

    shard_data = [
        ('node-1-amazonaws.com', [46, 47], 900000000, 1),
        ('node-1-amazonaws.com', [48, 49], 100000000, 1),
        ('node-1-amazonaws.com', [50, 51], 40000000, 1),
        ('node-1-amazonaws.com', [52, 53], 160000000, 1),
        ('node-1-amazonaws.com', [54, 55], 800000000, 1),
        ('node-1-amazonaws.com', [56, 57], 1000000000, 1),
        ('node-1-amazonaws.com', [58, 59], 500000000, 1),
        ('node-1-amazonaws.com', [60, 61], 500000000, 1),

        ('node-2-amazonaws.com', [15, 16], 450000000, 1),
        ('node-2-amazonaws.com', [17, 18], 50000000, 1),
        ('node-2-amazonaws.com', [19, 20], 20000000, 1),
        ('node-2-amazonaws.com', [21, 22], 80000000, 1),
        ('node-2-amazonaws.com', [23, 24], 400000000, 1),
        ('node-2-amazonaws.com', [25, 26], 500000000, 1),
        ('node-2-amazonaws.com', [27, 28], 250000000, 1),
        ('node-2-amazonaws.com', [29, 30], 250000000, 1),

        ('node-3-amazonaws.com', [31, 32], 1000000000, 1),
        ('node-3-amazonaws.com', [33, 34], 600000000, 1),
        ('node-3-amazonaws.com', [35, 36], 200000000, 1),
        ('node-3-amazonaws.com', [36, 37], 600000000, 1),
        ('node-3-amazonaws.com', [38, 39], 80000000, 1),
        ('node-3-amazonaws.com', [40, 41], 20000000 , 1),
        ('node-3-amazonaws.com', [42, 43], 250000000, 1),
        ('node-3-amazonaws.com', [44, 45], 250000000, 1),
    ]

    formation = init_formation(9000000000, nodes_data, shard_data, 10)

    assert formation.ideal_node_size == 2250000000.0
    assert formation.max_node_size == 3150000000.0
    assert formation.min_node_size == 1350000000.0

    assert len(formation.small_nodes) == 1
    assert len(formation.big_nodes) == 1
    assert formation.small_nodes[0].name == 'node-4-amazonaws.com'

    formation, moved = find_necessary_moves(formation)

    small_node = get_node_by_name('node-4-amazonaws.com', formation.nodes)
    assert small_node.size == 2500000000
    assert small_node.original_size == 0

    big_node = get_node_by_name('node-1-amazonaws.com', formation.nodes)
    assert big_node.size == 1500000000
    assert big_node.original_size == 4000000000

    for group in moved:
        assert group.to_node == small_node
        assert group.node == big_node


def test_find_necessary_moves_unbalanced_cluster():
    nodes_data = [('node-1-amazonaws.com', 1000000000), ('node-2-amazonaws.com', 2000000000),
                  ('node-3-amazonaws.com', 3000000000), ('node-4-amazonaws.com', 4000000000)]


    shard_data = [
        ('node-1-amazonaws.com', [1, 2], 200000000, 1),
        ('node-1-amazonaws.com', [3, 4], 50000000, 1),
        ('node-1-amazonaws.com', [5, 6], 50000000, 1),
        ('node-1-amazonaws.com', [5, 6], 100000000, 1),
        ('node-1-amazonaws.com', [7, 8], 300000000, 1),
        ('node-1-amazonaws.com', [9, 10], 100000000, 1),
        ('node-1-amazonaws.com', [11, 12], 150000000, 1),
        ('node-1-amazonaws.com', [13, 14], 50000000, 1),

        ('node-2-amazonaws.com', [15, 16], 450000000, 1),
        ('node-2-amazonaws.com', [17, 18], 50000000, 1),
        ('node-2-amazonaws.com', [19, 20], 20000000, 1),
        ('node-2-amazonaws.com', [21, 22], 80000000, 1),
        ('node-2-amazonaws.com', [23, 24], 400000000, 1),
        ('node-2-amazonaws.com', [25, 26], 500000000, 1),
        ('node-2-amazonaws.com', [27, 28], 250000000, 1),
        ('node-2-amazonaws.com', [29, 30], 250000000, 1),

        ('node-3-amazonaws.com', [31, 32], 1000000000, 1),
        ('node-3-amazonaws.com', [33, 34], 600000000, 1),
        ('node-3-amazonaws.com', [35, 36], 200000000, 1),
        ('node-3-amazonaws.com', [36, 37], 600000000, 1),
        ('node-3-amazonaws.com', [38, 39], 80000000, 1),
        ('node-3-amazonaws.com', [40, 41], 20000000 , 1),
        ('node-3-amazonaws.com', [42, 43], 250000000, 1),
        ('node-3-amazonaws.com', [44, 45], 250000000, 1),

        ('node-4-amazonaws.com', [46, 47], 900000000, 1),
        ('node-4-amazonaws.com', [48, 49], 100000000, 1),
        ('node-4-amazonaws.com', [50, 51], 40000000, 1),
        ('node-4-amazonaws.com', [52, 53], 160000000, 1),
        ('node-4-amazonaws.com', [54, 55], 800000000, 1),
        ('node-4-amazonaws.com', [56, 57], 1000000000, 1),
        ('node-4-amazonaws.com', [58, 59], 500000000, 1),
        ('node-4-amazonaws.com', [60, 61], 500000000, 1),
    ]

    formation = init_formation(10000000000, nodes_data, shard_data, 3)

    assert formation.ideal_node_size == 2500000000.0
    assert formation.min_node_size == 2200000000.0
    assert formation.max_node_size == 2800000000.0

    assert len(formation.big_nodes) == 2
    assert len(formation.small_nodes) == 2

    for node in formation.big_nodes:
        assert node.name in ('node-3-amazonaws.com', 'node-4-amazonaws.com')

    for node in formation.small_nodes:
        assert node.name in ('node-1-amazonaws.com', 'node-2-amazonaws.com')

    formation, moved = find_necessary_moves(formation)

    node1 = get_node_by_name('node-1-amazonaws.com', formation.nodes)
    assert node1.size == 2800000000
    assert node1.original_size == 1000000000

    node2 = get_node_by_name('node-2-amazonaws.com', formation.nodes)
    assert node2.size == 2700000000
    assert node2.original_size == 2000000000

    node3 = get_node_by_name('node-3-amazonaws.com', formation.nodes)
    assert node3.size == 2200000000
    assert node3.original_size == 3000000000

    node4 = get_node_by_name('node-4-amazonaws.com', formation.nodes)
    assert node4.size == 2300000000
    assert node4.original_size == 4000000000

    for group in moved:
        assert group.node.name in ('node-3-amazonaws.com', 'node-4-amazonaws.com')
        assert group.to_node.name in ('node-1-amazonaws.com', 'node-2-amazonaws.com')
