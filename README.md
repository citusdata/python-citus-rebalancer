# python-citus-rebalancer
A python command to get recommendations on commands to run to rebalance an unbalaned citus cluster. 

Sometimes when your cluster grows, you can have an uneven size on nodes: for example if one customers ingests more data than other. This simple commands helps you figure out what `master_move_shard` commands to run on your coordinator.

## Installation
### Install with pip install

1. Run `pip install py-citus-rebalancer`

### Install from source code

1. Clone the github repository
2. Run `python setup.py develop`


## Running the command

This command has different options:

- `--host`: is your postgres connection string
- `--delta`: is the maximum difference you expect between the node size and the idea size. For example if you have 4 nodes and 20GB of data, the idea size would be 5GB per node. The delta that you can give is for example 10% difference. It's an integer representing the percent.
- `--file`: path to the file where you want the command to be written: example `/path/to/rebalance.sql`. You can then run the file on your coordinator

1. Run the command `citus-rebalancer --host=postgres://user:pwd@host:5432/dbname --delta=10`
2. It will return commands to run, run them  on your coordinator to rebalance the cluster.
