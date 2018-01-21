# repo6
Raft implementation for CS 190, by Alex Jin and Don Mai

To start up a raft cluster of n servers:
1. Compile the Raft jar.
```bash
cd <raftProjectDirectoryPath>
sh create-raft-jar.sh
```

2. For each server that you want to start up in the cluster:
   1. Open a new terminal session.
   2. cd into the directory where you want the server's persistent state log file to be stored. You can reuse this directory to store the log files of multiple servers since the name of the log files are based on server id.
   3. Run the following:
       ```bash
       export RAFT_PROJECT_PATH=<raftProjectDirectoryPath>
       java -jar $RAFT_PROJECT_PATH/RaftServer.jar <myPortIndex> <port0>,<port1>,<port2>
       ```
    Note that the list of ports that you supply is 0-indexed.

Example usage for a cluster of 2 servers:
```bash
# Open a terminal session and run these commands:
export RAFT_PROJECT_PATH="/Users/donasaur/Documents/workspace-eclipse/190RaftProject"
cd $RAFT_PROJECT_PATH
sh create-raft-jar.sh

# Start up server 0 using the same terminal session:
java -jar $RAFT_PROJECT_PATH/RaftServer.jar 0 6060,6061

# Open another terminal session and start up server 1:
export RAFT_PROJECT_PATH="/Users/donasaur/Documents/workspace-eclipse/190RaftProject"
java -jar $RAFT_PROJECT_PATH/RaftServer.jar 1 6060,6061
```
