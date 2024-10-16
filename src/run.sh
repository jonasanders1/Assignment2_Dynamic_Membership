#!/bin/bash

if [ -z "$1" ]; then
  echo "Usage: $0 <number_of_servers>"
  exit 1
fi

NUM_SERVERS=$1
HOSTS=($(/share/ifi/available-nodes.sh))  # Get all the available nodes
HOST_PORTS=()  # Store host:port combos
PROJECT_DIR=$PWD
BASE_PORT=6000

# Start the servers
for ((i=0; i<$NUM_SERVERS; i++)); do
  HOST=${HOSTS[$i % ${#HOSTS[@]}]}
   PORT=$((BASE_PORT + i))

  HOST_PORT="$HOST:$PORT"
  HOST_PORTS+=("$HOST_PORT")
  
  echo "Starting server on $HOST:$PORT"
  ssh -n -f $HOST "source $PROJECT_DIR/venv/bin/activate && nohup python3 $PROJECT_DIR/Node.py $PORT > $PROJECT_DIR/server_$PORT.log 2>&1 &"
done


# Wait for all nodes to start
sleep 5

# Loop over all and join via join endpoint
FIRST_NODE=${HOST_PORTS[0]}
for ((i=1; i<$NUM_SERVERS; i++)); do
  HOST_PORT=${HOST_PORTS[$i]}
  echo "Joining $HOST_PORT to the network through $FIRST_NODE"
  curl -X POST "http://$HOST_PORT/join?nprime=$FIRST_NODE"
done

# Print the nodes list
echo "Network nodes:"
printf '%s\n' "${HOST_PORTS[@]}"

# Ensure the script exits without hanging
exit 0