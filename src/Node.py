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
        self.successor = self.address
        self.predecessor = None
        self.data_store = {}
        self.finger_table = []
        
        print(f"Initializing node with address {self.address} and ID hash {self.node_id}", flush=True)


    
    # function to join a network throuth a nprime
    def join(self, nprime_address):
        if nprime_address == self.address:
            # This node is the first node in the network
            self.predecessor = None
            self.successor = self.address
            return

        try:
            # Find the correct position for this node
            self.successor = self.find_successor(self.node_id, nprime_address)
            
            # Get and update this node's predecessor
            response = requests.get(f"http://{self.successor}/predecessor")
            response.raise_for_status()
            self.predecessor = response.json()['predecessor']
            
            # Notify successor and predecessor about the updates
            if self.successor:
                response = requests.post(f"http://{self.successor}/update-predecessor", json={'predecessor': self.address})
                response.raise_for_status()

            if self.predecessor:
                response = requests.post(f"http://{self.predecessor}/update-successor", json={'successor': self.address})
                response.raise_for_status()

            # Transfer keys if necessary
            # self.transfer_keys()

            # Update finger table
            self.update_finger_table()

            print(f"Node {self.address} joined the network through {nprime_address}", flush=True)
        except Exception as e:
            print(f"Error joining network through {nprime_address}: {e}", flush=True)


    # function that handles the process if leaving the network
    def leave(self):
        try:
            # Notify predecessor to update its successor to this node's successor
            if self.predecessor and self.predecessor != self.address:
                print(f"Notifying predecessor {self.predecessor} to update successor to {self.successor}", flush=True)
                requests.post(f"http://{self.predecessor}/update-successor", json={'successor': self.successor})

            # Notify successor to update its predecessor to this node's predecessor
            if self.successor and self.successor != self.address:
                print(f"Notifying successor {self.successor} to update predecessor to {self.predecessor}", flush=True)
                requests.post(f"http://{self.successor}/update-predecessor", json={'predecessor': self.predecessor})

            # Reset node to single-node state (it is no longer part of the DHT ring)
            self.successor = self.address
            self.predecessor = None
            print(f"Node {self.address} has left the network and reset to single-node state.", flush=True)

        except Exception as e:
            print(f"Error during leave: {e}", flush=True)

    
    def stabilize(self):
        """Periodically checks the successor's predecessor and updates if needed."""
        try:
            # Get successor's predecessor
            response = requests.get(f"http://{self.successor}/predecessor")
            response.raise_for_status()
            successor_predecessor = response.json()['predecessor']

            # If the successor's predecessor is between this node and the successor, update the successor
            if successor_predecessor and hash_value(successor_predecessor) > hash_value(self.address) and hash_value(successor_predecessor) < hash_value(self.successor):
                self.successor = successor_predecessor

            # Notify the successor about this node
            response = requests.post(f"http://{self.successor}/update-predecessor", json={'predecessor': self.address})
            response.raise_for_status()

            print(f"Stabilization complete for node {self.address}. Successor is {self.successor}", flush=True)
        except Exception as e:
            print(f"Error in stabilization: {e}", flush=True)



    def find_successor(self, key_hash, start_node=None):
        """Find the successor of the given key hash."""
        if start_node is None:
            start_node = self.address

        try:
            response = requests.get(f"http://{start_node}/node-info")
            response.raise_for_status()
            node_info = response.json()

            current_node_hash = hash_value(node_info['address'])
            successor_hash = hash_value(node_info['successor'])

            # Check if the key falls between the current node and its successor
            if (current_node_hash < key_hash <= successor_hash) or \
            (current_node_hash > successor_hash and (key_hash > current_node_hash or key_hash <= successor_hash)):
                return node_info['successor']
            else:
                closest_preceding_node = self.find_closest_preceding_node(key_hash, node_info)
                if closest_preceding_node == node_info['address']:
                    return node_info['successor']
                return self.find_successor(key_hash, closest_preceding_node)
        except Exception as e:
            print(f"Error in find_successor: {e}", flush=True)
            return None


    def find_closest_preceding_node(self, key_hash, node_info):
        """Find the closest preceding node in the finger table for a given key hash."""
        for finger in reversed(node_info['finger_table']):
            if hash_value(node_info['address']) < hash_value(finger) < key_hash:
                return finger
        return node_info['address']

    # def transfer_keys(self):
    #     """Transfer keys that should now belong to this node."""
    #     if self.successor != self.address:
    #         try:
    #             response = requests.get(f"http://{self.successor}/transfer-keys/{self.node_id}")
    #             keys_to_transfer = response.json()
    #             for key, value in keys_to_transfer.items():
    #                 self.data_store[key] = value
    #             print(f"Transferred {len(keys_to_transfer)} keys from successor", flush=True)
    #         except Exception as e:
    #             print(f"Error transferring keys from successor: {e}", flush=True)

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

    def put(self, key, value):
        """Store a key-value pair in the DHT."""
        key_hash = hash_value(key)
        print(f"Storing key: {key}, hash: {key_hash} at node {self.address}", flush=True)

        responsible_node = self.find_successor(key_hash)

        if responsible_node == self.address:
            self.data_store[key] = value
            print(f"Data stored locally at {self.address} for key: {key}", flush=True)
            return "Stored locally"
        else:
            try:
                response = requests.put(f"http://{responsible_node}/storage/{key}", data=value)
                response.raise_for_status()
                return response.text
            except Exception as e:
                print(f"Error forwarding PUT to {responsible_node}: {e}", flush=True)
                return str(e)

    def get(self, key):
        """Retrieve a value for a given key from the DHT."""
        key_hash = hash_value(key)
        print(f"Retrieving key: {key}, hash: {key_hash} from node {self.address}", flush=True)

        responsible_node = self.find_successor(key_hash)

        if responsible_node == self.address:
            value = self.data_store.get(key)
            if value is not None:
                print(f"Found key {key} in node {self.address}", flush=True)
                return value
            else:
                print(f"Key {key} not found in node {self.address}", flush=True)
                return None
        else:
            try:
                response = requests.get(f"http://{responsible_node}/storage/{key}", timeout=5)
                response.raise_for_status()
                return response.text
            except requests.exceptions.RequestException as e:
                print(f"Error during GET request to {responsible_node}: {e}", flush=True)
                return None

# Flask Routes
@app.route('/join', methods=['POST'])
def join_network():
    nprime = request.args.get('nprime')
    if nprime:
        node1.join(nprime)
        return jsonify({'message': f'Joined network through {nprime}'}), 200
    else:
        return jsonify({'error': 'No nprime specified'}), 400


@app.route('/leave', methods=['POST'])
def leave_network():
    node1.leave()
    return jsonify({'message': 'Node has left the network'}), 200



@app.route('/node-info', methods=['GET'])
def get_node_info():
    return jsonify({
        'address': node1.address,
        'node_hash': node1.node_id,
        'successor': node1.successor,
        'predecessor': node1.predecessor,
        'finger_table': node1.finger_table
    }), 200

@app.route('/update-predecessor', methods=['POST'])
def update_predecessor():
    new_predecessor = request.json['predecessor']
    node1.predecessor = new_predecessor
    return jsonify({'message': 'Predecessor updated'}), 200

@app.route('/update-successor', methods=['POST'])
def update_successor():
    new_successor = request.json['successor']
    node1.successor = new_successor
    return jsonify({'message': 'Successor updated'}), 200

@app.route('/predecessor', methods=['GET'])
def get_predecessor():
    return jsonify({'predecessor': node1.predecessor}), 200

@app.route('/successor', methods=['GET'])
def get_successor():
    return jsonify({'successor': node1.successor}), 200

# @app.route('/transfer-keys/<int:node_id>', methods=['GET'])
# def transfer_keys(node_id):
#     keys_to_transfer = {}
#     for key, value in list(node1.data_store.items()):
#         if hash_value(key) <= node_id:
#             keys_to_transfer[key] = value
#             del node1.data_store[key]
#     return jsonify(keys_to_transfer), 200

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

    # Initialize the node
    node1 = Node(address=node_address) 
    print(f"Initializing node with address: {node_address}", flush=True)

    # Start stabilization in a separate thread
    import threading
    import time

    def stabilization_task():
        while True:
            node1.stabilize()
            time.sleep(10)  # Run stabilize every 10 seconds

    # Start stabilization in a background thread
    thread = threading.Thread(target=stabilization_task)
    thread.daemon = True  # Daemon thread exits when the main program exits
    thread.start()

    # Start the Flask server
    app.run(host="0.0.0.0", port=port)

