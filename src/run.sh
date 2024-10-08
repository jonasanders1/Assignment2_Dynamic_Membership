#!/bin/bash

# Ensure correct usage
if [ -z "$1" ]; then
  echo "Usage: $0 <number_of_servers>"
  exit 1
fi

NUM_SERVERS=$1
HOSTS=($(/share/ifi/available-nodes.sh))  # Get available nodes
HOST_PORTS=()  # Store host:port combos
PROJECT_DIR=$PWD

# Start the servers
for ((i=0; i<$NUM_SERVERS; i++)); do
  HOST=${HOSTS[$i % ${#HOSTS[@]}]}
  PORT=$(shuf -i 5000-60000 -n 1)

  HOST_PORT="$HOST:$PORT"
  HOST_PORTS+=("$HOST_PORT")
  
  echo "Starting server on $HOST:$PORT"
  ssh -n -f $HOST "source $PROJECT_DIR/venv/bin/activate && nohup python3 $PROJECT_DIR/Node.py $PORT > $PROJECT_DIR/server_$PORT.log 2>&1 &"
done

# Convert known nodes to JSON array (correctly formatted)
KNOWN_NODES_JSON=$(printf '%s\n' "${HOST_PORTS[@]}" | jq -R . | jq -s .)

# Output the host:port combinations as a JSON list
echo "Known Nodes JSON: $KNOWN_NODES_JSON"

# Now that all nodes are running, inform them of each other
for HOST_PORT in "${HOST_PORTS[@]}"; do
  IFS=":" read -r HOST PORT <<< "$HOST_PORT"
  echo "Updating node at $HOST:$PORT with known nodes"

  # Send POST request to update known nodes and capture response
  RESPONSE=$(curl -s -o /dev/null -w "%{http_code}" -X POST -H "Content-Type: application/json" -d "{\"nodes\": $KNOWN_NODES_JSON}" http://$HOST:$PORT/network)
  
  if [ "$RESPONSE" -eq 200 ]; then
    echo "Successfully updated $HOST:$PORT"
  else
    echo "Failed to update $HOST:$PORT, HTTP status code: $RESPONSE"
    
    # Optionally retry the POST request once if it failed
    echo "Retrying update for $HOST:$PORT..."
    sleep 2
    RETRY_RESPONSE=$(curl -s -o /dev/null -w "%{http_code}" -X POST -H "Content-Type: application/json" -d "{\"nodes\": $KNOWN_NODES_JSON}" http://$HOST:$PORT/network)
    if [ "$RETRY_RESPONSE" -eq 200 ]; then
      echo "Retry successful for $HOST:$PORT"
    else
      echo "Retry failed for $HOST:$PORT, HTTP status code: $RETRY_RESPONSE"
    fi
  fi
done

# Print the final host:port list
echo $(printf '%s\n' "${HOST_PORTS[@]}" | jq -R . | jq -s .)

# Ensure the script exits without hanging
exit 0
