#!/bin/bash

if [ -z "$1" ]; then
  echo "Usage: $0 <number_of_servers>"
  exit 1
fi

NUM_SERVERS=$1
HOSTS=($(/share/ifi/available-nodes.sh))  # Get all the available nodes
HOST_PORTS=()  # Store host:port combos
PROJECT_DIR=$PWD

# Start the servers
for ((i=0; i<$NUM_SERVERS; i++)); do
  HOST=${HOSTS[$i % ${#HOSTS[@]}]}
  PORT=$(shuf -i 49152-65535 -n 1)

  HOST_PORT="$HOST:$PORT"
  HOST_PORTS+=("$HOST_PORT")
  
  echo "Starting server on $HOST:$PORT"
  ssh -n -f $HOST "source $PROJECT_DIR/venv/bin/activate && nohup python3 $PROJECT_DIR/Node.py $PORT > $PROJECT_DIR/server_$PORT.log 2>&1 &"
done

# Wait for all nodes to start
sleep 5

# Print the nodes list in JSON format
NODES_LIST=$(printf '"%s",' "${HOST_PORTS[@]}")
NODES_LIST="[${NODES_LIST%,}]"

echo "Network nodes in JSON format:"
echo "$NODES_LIST"

# Ensure the script exits without hanging
exit 0
