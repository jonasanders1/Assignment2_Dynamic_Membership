import requests
import sys
from flask import Flask, request, jsonify, Response
import hashlib
import socket

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
        
        # log the current node's initialization
        print(f"Initializing node with address {self.address} and ID hash {self.node_id}", flush=True)

    def update_successor_predecessor(self, node_list):
        """Update successor and predecessor based on the sorted node list, then drop the node list."""
        
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
        self_hash = self.node_id

        index = sorted_nodes.index(self.address)
        self.successor = sorted_nodes[(index + 1) % len(sorted_nodes)]
        self.predecessor = sorted_nodes[(index - 1) % len(sorted_nodes)]

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
        
        # populate the finger table
        for i in range(m):
            start = (self.node_id + 2**i) % (2**m)
            successor = self.find_successor(start)
            
            if successor and successor not in self.finger_table:
                self.finger_table.append(successor)
        print(f"Finger table for node {self.address} updated: {self.finger_table}", flush=True)

    def find_successor(self, key_hash):
        """Find the successor of the given key hash using finger table and neighbors."""
        if self.predecessor and self.predecessor not in self.node_hashes:
            self.node_hashes[self.predecessor] = hash_value(self.predecessor)

        # If the key is between this node and its successor, return the successor
        if self.predecessor is None or (self.node_hashes[self.predecessor] < key_hash <= self.node_id):
            return self.address

        # Use finger table to find the closest node to the key
        find_closest_node = self.find_closest_node(key_hash)
        if find_closest_node:
            return find_closest_node

        # Fallback to successor if no closer node is found
        return self.successor if self.successor != self.address else None

    def find_closest_node(self, key_hash):
        """ Find the closest preceding node in the finger table for a given key hash. """
        for i in reversed(range(len(self.finger_table))):
            finger_node_hash = hash_value(self.finger_table[i])
            if self.node_id < finger_node_hash < key_hash:
                return self.finger_table[i]
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
            return "Stored locally"

        # Find the closest preceding node using the finger table
        closest_node = self.find_closest_node(key_hash)

        # If the closest node is this node itself, store locally
        if closest_node == self.address:
            self.data_store[key_hash] = value
            print(f"Data stored locally at {self.address} as closest node.", flush=True)
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
            # print(f"Forwarding GET request to {closest_node} for key {key} With flask", flush=True)
            # response = get_value()
            
            response.raise_for_status()
            return response.text
        except requests.exceptions.Timeout:
            print(f"Request to {closest_node} timed out.", flush=True)
            return None
        except requests.exceptions.RequestException as e:
            print(f"Error during GET request to {closest_node}: {e}", flush=True)
            return None



# Flask Routes
@app.route('/network', methods=['POST'])
def network_update():
    node_list = request.json['nodes']
    if node1.address not in node_list:
        print(f"Adding current node {node1.address} to node_list.", flush=True)
        node_list.append(node1.address)
    
    node1.update_successor_predecessor(node_list)
    
    return jsonify({'message': 'Updated network'}), 200


@app.route('/storage/<key>', methods=['PUT'])
def put_value(key):
    value = request.data.decode('utf-8')
    response = node1.put(key, value)
    return Response(response, content_type='text/plain'), 200  


@app.route('/storage/<key>', methods=['GET'])
def get_value(key):
    value = node1.get(key)
    if value is not None:
        return Response(value, content_type='text/plain'), 200
    else:
        return Response("Key not found", content_type='text/plain'), 404



# ! Helper endpoints
@app.route('/successor', methods=['GET'])
def get_successor():
    return jsonify({'successor': node1.successor}), 200

@app.route('/predecessor', methods=['GET'])
def get_predecessor():
    return jsonify({'predecessor': node1.predecessor}), 200

@app.route('/fingertable', methods=['GET'])
def get_finger_table():
    return jsonify({'fingertable': node1.finger_table}), 200

@app.route('/helloworld', methods=['GET'])
def helloworld():
    return node1.address, 200


if __name__ == '__main__':
    port = int(sys.argv[1])
    hostname = socket.gethostname().split('.')[0]  
    node_address = f"{hostname}:{port}"
    node1 = Node(address=node_address) 
    print(f"Initializing node with address: {node_address}", flush=True)
    app.run(host="0.0.0.0", port=port)
