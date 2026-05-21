CHAIN DATA FOLDER
=================

This folder holds the blockchain data for your BlockDAG nodes.

  chain-data\node1\     - Chain data for node 1
  chain-data\node2\     - Chain data for node 2
  chain-data\postgres\  - Pool database data

These folders are populated automatically once the node stack is running
and the nodes begin syncing from the network.

PRE-SEEDING (optional)
-----------------------
If you have a chain data snapshot (chain-data-seed.zip), you can extract
its contents into node1\ and node2\ before starting the stack to avoid
syncing from block 0. This can save many hours of sync time.

NOTE: Do not delete this folder. Docker mounts it directly into the node
containers. If it is missing, Docker will create it as root and the nodes
may fail to write their data correctly.
