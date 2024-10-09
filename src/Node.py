import requests
import sys
from flask import Flask, request, jsonify, Response
import hashlib
import socket
import time
app = Flask(__name__)

# hash function
def hash_value(value):
    print(f"Hashing value: {value}", flush=True)
    return int(hashlib.sha1(value.encode()).hexdigest(), 16)

# represents a node in the DHT
class Node:
    
    # initializing a node
    def __init__(self, address):
        self.node_id = hash_value(address)
        self.address = address
        self.successor = None
        self.predecessor = None
        self.data_store = {}
        self.finger_table = []
        self.node_hashes = {}
        self.is_crashed = False
        
        # log the current node's initialization
        print(f"Initializing node with address {self.address} and ID hash {self.node_id}", flush=True)

    # Function to join the network
    def join(self, known_node_address):
        if known_node_address is None:
            # First node in the network: Set successor and predecessor to self
            self.successor = self.address
            self.predecessor = self.address
            print(f"Node {self.address} is the first node in the network.", flush=True)
            self.update_finger_table()
        else:
            retries = 3
            while retries > 0:
                try:
                    print(f"Attempting to join the network via {known_node_address}", flush=True)
                    
                    # Join via known node (find successor)
                    response = requests.post(f'http://{known_node_address}/find_successor', json={"id": self.node_id})
                    if response.status_code == 200:
                        self.successor = response.json().get('successor')
                        print(f"Joined network via {known_node_address}, successor is {self.successor}", flush=True)
                        
                        # Notify the successor about this node's existence
                        self.notify_successor(self.successor)
                        
                        # Fetch predecessor from the successor
                        print(f"Fetching predecessor from successor {self.successor}", flush=True)
                        pred_response = requests.get(f'http://{self.successor}/predecessor')
                        if pred_response.status_code == 200:
                            predecessor = pred_response.json().get('predecessor')
                            if predecessor:
                                print(f"Predecessor received from {self.successor}: {predecessor}", flush=True)
                                self.predecessor = predecessor
                            else:
                                print(f"No valid predecessor received from {self.successor}, keeping predecessor as None.", flush=True)
                        else:
                            print(f"Failed to fetch predecessor from {self.successor}, status code: {pred_response.status_code}", flush=True)
                        
                        # Update finger table after joining
                        self.update_finger_table()
                        break  # Successfully joined, exit loop
                    else:
                        print(f"Failed to join via {known_node_address}. Response: {response.status_code}", flush=True)
                except requests.exceptions.RequestException as e:
                    print(f"Error joining the network: {e}. Retrying...", flush=True)
                
                retries -= 1
                if retries == 0:
                    print(f"Unable to join via {known_node_address} after retries. Falling back to self-loop.", flush=True)
                    self.successor = self.address
                    self.predecessor = self.address
                    self.update_finger_table()



    # Notify the successor about the new joining node
    def notify_successor(self, successor_address):
        if successor_address and successor_address != self.address:
            try:
                response = requests.post(f'http://{successor_address}/notify', json={"predecessor": self.address})
                print(f"Notified successor {successor_address} about predecessor {self.address}: {response.status_code}", flush=True)
            except Exception as e:
                print(f"Error notifying successor {successor_address}: {e}", flush=True)
        else:
            print(f"No notification sent to successor {successor_address}.", flush=True)


    def update_successor_predecessor(self, node_list):
        """Update successor and predecessor based on the sorted node list, then drop the node list."""
        print(f"Updating successor/predecessor with node list: {node_list}", flush=True)
        
        # cache node hashes to avoid redundant hashing
        for node in node_list:
            if node not in self.node_hashes:
                self.node_hashes[node] = hash_value(node)
                print(f"Hashed and added node {node} with hash {self.node_hashes[node]}", flush=True)

        # ensure the current node's address is part of the known nodes
        if self.address not in node_list:
            print(f"Adding current node {self.address} to the known nodes list.", flush=True)
            node_list.append(self.address)
            self.node_hashes[self.address] = self.node_id

        # sort based on hash values and update successor/predecessor
        sorted_nodes = sorted(node_list, key=lambda node: hash_value(node))
        print(f"Sorted nodes based on hashes: {sorted_nodes}", flush=True)
        self_hash = self.node_id

        index = sorted_nodes.index(self.address)
        self.successor = sorted_nodes[(index + 1) % len(sorted_nodes)]
        self.predecessor = sorted_nodes[(index - 1) % len(sorted_nodes)]

        print(f"Updated successor: {self.successor}, predecessor: {self.predecessor} for node {self.address}", flush=True)

        # after setting successor and predecessor, drop the full node list
        print(f"Dropping known nodes list after setting up the ring.", flush=True)

        # update finger table after setting successor and predecessor
        self.update_finger_table()

        # clear known_nodes to make sure it is not used after the setup
        self.node_hashes = {}

    def get_address_by_hash(self, node_hash):
        """Helper function to get the address corresponding to a node hash."""
        for node, hashed_value in self.node_hashes.items():
            if hashed_value == node_hash:
                return node
        return None

    def update_finger_table(self):
        """Updates the finger table for a node."""
        m = 160  # number of finger entries due to SHA-1 hashing
        self.finger_table = []
        print(f"Updating finger table for node {self.address}...", flush=True)
        updated_entries = []
        for i in range(m):
            start = (self.node_id + 2**i) % (2**m)
            successor = self.find_successor(start)
            
            if successor and successor not in self.finger_table:
                self.finger_table.append(successor)
                updated_entries.append((i, start, successor))

        print(f"Finger table updated with {len(updated_entries)} entries.", flush=True)
        if len(updated_entries) > 5:
            print(f"Sample entries: {updated_entries[:5]}... (truncated)", flush=True)

    def find_successor(self, key_hash):
        """Find the successor of the given key hash using the finger table and neighbors."""
        print(f"Finding successor for key_hash {key_hash} at node {self.address}", flush=True)

        # Check if this node has a valid successor before proceeding
        if not self.successor or self.successor == self.address:
            print(f"Error: Node {self.address} does not have a valid successor.", flush=True)
            return None

        # Now proceed with finding the successor
        if self.predecessor and (self.node_id >= key_hash > hash_value(self.predecessor)):
            return self.address

        # Use finger table or neighbors to find successor
        closest_node = self.find_closest_node(key_hash)
        if closest_node and closest_node != self.address:
            try:
                # Contact closest node to find successor
                response = requests.post(f'http://{closest_node}/find_successor', json={"id": key_hash})
                if response.status_code == 200:
                    return response.json().get('successor')
                else:
                    print(f"Error finding successor at {closest_node}, received {response.status_code}", flush=True)
            except requests.exceptions.RequestException as e:
                print(f"Error while contacting {closest_node}: {e}", flush=True)

        # If no closer node is found, return the current successor
        return self.successor


    def find_closest_node(self, key_hash, retry_limit=5):
        """ Find the closest preceding node in the finger table for a given key hash. """
        print(f"Finding closest node in finger table for key_hash {key_hash}", flush=True)
        
        for i in reversed(range(len(self.finger_table))):
            finger_node_hash = hash_value(self.finger_table[i])
            if self.node_id < finger_node_hash < key_hash:
                print(f"Closest node found: {self.finger_table[i]} with hash {finger_node_hash}", flush=True)
                return self.finger_table[i]

        # If no closer node is found, return successor
        return self.successor


    # function to store a key-value pair in the node
    def put(self, key, value):
        # hashing the key
        key_hash = hash_value(key)
        print(f"Storing key: {key}, hash: {key_hash} at node {self.address}", flush=True)

        # Check if the current node is responsible for storing the key
        if (self.predecessor is None or 
            (hash_value(self.predecessor) < key_hash <= self.node_id) or 
            (self.node_id < hash_value(self.predecessor) and (key_hash > hash_value(self.predecessor) or key_hash <= self.node_id))):
            
            self.data_store[key_hash] = value
            print(f"Data stored locally at {self.address} for key_hash: {key_hash}", flush=True)
            print(f"Data store state: {self.data_store}", flush=True)  # Debug print for data store state
            return "Stored locally"

        # Find the closest preceding node using the finger table
        closest_node = self.find_closest_node(key_hash)

        # If the closest node is this node itself, store locally
        if closest_node == self.address:
            self.data_store[key_hash] = value
            print(f"Data stored locally at {self.address} as closest node.", flush=True)
            print(f"Data store state: {self.data_store}", flush=True)  # Debug print for data store state
            return "Stored locally"

        try:
            # Forward the PUT request to the closest node found
            print(f"Forwarding PUT request to {closest_node} for key {key}", flush=True)
            response = requests.put(f"http://{closest_node}/storage/{key}", data=value)
            print(f"Response from closest node {closest_node}: {response.text}", flush=True)
            return response.text
        except Exception as e:
            print(f"Error forwarding to {closest_node}: {e}", flush=True)
            return str(e)


    # function to get a value based on a given key
    def get(self, key):
        # hashing the key
        key_hash = hash_value(key)
        
        print(f"Retrieving key: {key}, hash: {key_hash} from node {self.address}", flush=True)

        # Check if the key is stored locally
        if key_hash in self.data_store:
            print(f"Found key {key} in node {self.address}", flush=True)
            return self.data_store[key_hash]

        # Find the closest preceding node using the finger table
        closest_node = self.find_closest_node(key_hash)

        # If the closest node is this node itself, the key isn't found locally
        if closest_node == self.address:
            print(f"Key {key} not found in node {self.address}", flush=True)
            return None

        try:
            # Forward the GET request to the closest node found
            print(f"Forwarding GET request to {closest_node} for key {key}", flush=True)
            response = requests.get(f"http://{closest_node}/storage/{key}", timeout=5)
            print(f"Response from GET request to {closest_node}: {response.status_code}, {response.text}", flush=True)
            response.raise_for_status()
            return response.text
        except requests.exceptions.Timeout:
            print(f"Request to {closest_node} timed out.", flush=True)
            return None
        except requests.exceptions.RequestException as e:
            print(f"Error during GET request to {closest_node}: {e}", flush=True)
            return None



@app.route('/network', methods=['POST'])
def network_update():
    node_list = request.json['nodes']
    print(f"Received network update with nodes: {node_list}", flush=True)
    if node1.address not in node_list:
        print(f"Adding current node {node1.address} to node_list.", flush=True)
        node_list.append(node1.address)
    
    node1.update_successor_predecessor(node_list)
    
    return jsonify({'message': 'Updated network'}), 200

@app.route('/storage/<key>', methods=['PUT'])
def put_value(key):
    value = request.data.decode('utf-8')
    print(f"PUT request received for key {key}, value {value}", flush=True)
    response = node1.put(key, value)
    return Response(response, content_type='text/plain'), 200  

@app.route('/storage/<key>', methods=['GET'])
def get_value(key):
    print(f"GET request received for key {key}", flush=True)
    value = node1.get(key)
    if value is not None:
        return Response(value, content_type='text/plain'), 200
    else:
        return Response("Key not found", content_type='text/plain'), 404

# * Assignment 2 ENDPOINTS
@app.route('/join', methods=['POST'])
def join_network():
    nprime = request.args.get('nprime')
    print(f"Received /join request with nprime {nprime}", flush=True)
    if not nprime:
        return jsonify({"error": "Missing nprime parameter"}), 400
    node1.join(nprime)
    return jsonify({"message": f"Joined the network via {nprime}, successor is {node1.successor}"}), 200

@app.route('/node-info', methods=['GET'])
def get_node_info():
    if node1.is_crashed:
        return jsonify({"error": "Node is crashed"}), 503
    response = {
        "node_hash": f"{node1.node_id:x}",
        "successor": node1.successor,
        "predecessor": [node1.predecessor] if node1.predecessor else [],
        "finger_table": node1.finger_table
    }
    print(f"Returning node info: {response}", flush=True)
    return jsonify(response), 200


# Notify predecessor update
@app.route('/notify', methods=['POST'])
def notify_predecessor():
    data = request.get_json()
    print(f"Received /notify request: Predecessor is {data['predecessor']}", flush=True)
    node1.predecessor = data['predecessor']
    print(f"Predecessor of node {node1.address} updated to {node1.predecessor}", flush=True)
    return jsonify({"message": "Predecessor updated"}), 200


@app.route('/find_successor', methods=['POST'])
def find_successor_route():
    data = request.get_json()
    node_id = data['id']
    print(f"Received /find_successor request for node_id {node_id}", flush=True)
    
    # Call the find_successor method in the Node class
    successor = node1.find_successor(node_id)
    
    if successor:
        return jsonify({"successor": successor}), 200
    else:
        print(f"Could not find successor for node_id: {node_id}", flush=True)
        return jsonify({"error": "Successor not found"}), 404

@app.route('/successor', methods=['GET'])
def get_successor():
    return jsonify({'successor': node1.successor}), 200

@app.route('/predecessor', methods=['GET'])
def get_predecessor():
    return jsonify({'predecessor': node1.predecessor}), 200

@app.route('/fingertable', methods=['GET'])
def get_finger_table():
    return jsonify({'fingertable': node1.finger_table}), 200



# Run node
if __name__ == '__main__':
    port = int(sys.argv[1])
    hostname = socket.gethostname().split('.')[0]  
    node_address = f"{hostname}:{port}"
    node1 = Node(address=node_address) 
    print(f"Initializing node with address: {node_address}", flush=True)
    app.run(host="0.0.0.0", port=port)
