# repo6
Raft implementation for CS 190, by Alex Jin and Don Mai

To start up m clients and a Raft cluster of n servers:
1. Compile the Raft client and Raft server jars.
```bash
cd <raftProjectDirectoryPath>
sh create-raft-jars.sh
```

2. Save the environment variable `RAFT_PROJECT_PATH=<raftProjectDirectoryPath>` in your shell configuration file or set the variable for each opened terminal session.

3. For each server that you want to start up in the cluster:
   1. Open a new terminal session.
   2. cd into the directory where you want the server's directory, containing the server's persistent state, to be housed. You can reuse this directory-housing directory to store the persistent state directories of multiple servers since the names of the persistent state directories are based on server id.
   3. Run the following:
       ```bash
       java -jar $RAFT_PROJECT_PATH/RaftServer.jar <hostname0:port0>,<hostname1:port1>,...,<hostname$n-1$,port$n-1$> <myAddressIndex>
       ```
    Note that the list of addresses that you supply is 0-indexed.

4. For each client that you want to start up:
   1. Open a new terminal session.
   2. Run the following:
       ```bash
       java -jar $RAFT_PROJECT_PATH/RaftServer.jar <myHostname:myPort> <hostname0:port0>,<hostname1:port1>,...,<hostname$n-1$,port$n-1$>
       ```

Example usage for starting up 2 clients and a cluster of 3 servers:
```bash
# Save the following environment variable in my shell configuration file:
export RAFT_PROJECT_PATH="/Users/donasaur/Documents/workspace-eclipse/190RaftProject"

# Open a terminal session and run these commands:
cd $RAFT_PROJECT_PATH
sh create-raft-jars.sh

# Start up server 0 using the same terminal session:
java -jar $RAFT_PROJECT_PATH/RaftServer.jar localhost:6060,localhost:6061,localhost:6062 0

# Open another terminal session and start up server 1:
java -jar $RAFT_PROJECT_PATH/RaftServer.jar localhost:6060,localhost:6061,localhost:6062 1

# Open another terminal session and start up server 2:
java -jar $RAFT_PROJECT_PATH/RaftServer.jar localhost:6060,localhost:6061,localhost:6062 2

# Open another terminal session and start up client 0:
java -jar $RAFT_PROJECT_PATH/RaftClient.jar localhost:6070 localhost:6060,localhost:6061,localhost:6062

# Open another terminal session and start up client 1:
java -jar $RAFT_PROJECT_PATH/RaftClient.jar localhost:6071 localhost:6060,localhost:6061,localhost:6062
```
