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
    def __init__(self, address, r = 8):
        self.node_id = hash_value(address)
        self.address = address
        self.successor = self.address
        self.predecessor = None
        self.data_store = {}
        self.finger_table = []
        self.crashed = False  # New flag to simulate a crash
        self.successor_list = [self.address] * r

        print(f"Initializing node with address {self.address} and ID hash {self.node_id}", flush=True)


    # function to join a network through a nprime
    def join(self, nprime_address):
        if self.crashed:
            return "Node is crashed and cannot join the network", 500

        if nprime_address == self.address:
            self.predecessor = None
            self.successor = self.address
            return

        try:

            self.successor = self.find_successor(self.node_id, nprime_address)
            
            response = requests.get(f"http://{self.successor}/predecessor")
            response.raise_for_status()
            self.predecessor = response.json()['predecessor']

            if self.successor:
                response = requests.post(f"http://{self.successor}/update-predecessor", json={'predecessor': self.address})
                response.raise_for_status()

            if self.predecessor:
                response = requests.post(f"http://{self.predecessor}/update-successor", json={'successor': self.address})
                response.raise_for_status()

            self.update_finger_table()

            print(f"Node {self.address} joined the network through {nprime_address}", flush=True)
        except Exception as e:
            print(f"Error joining network through {nprime_address}: {e}", flush=True)


    # function that handles the process of leaving the network
    def leave(self):
        if self.crashed:
            return "Node is crashed and cannot leave the network", 500

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

    
    # def stabilize(self):
    #     if self.crashed:
    #         return "Node is crashed and cannot stabilize", 500

    #     """Periodically checks the successor's predecessor and updates if needed."""
    #     try:
    #         # Attempt to get successor's predecessor
    #         response = requests.get(f"http://{self.successor}/predecessor", timeout=5)  # Timeout to detect failure
    #         response.raise_for_status()
    #         successor_predecessor = response.json()['predecessor']

    #         # If the successor's predecessor is between this node and the successor, update the successor
    #         if successor_predecessor and hash_value(successor_predecessor) > hash_value(self.address) and hash_value(successor_predecessor) < hash_value(self.successor):
    #             self.successor = successor_predecessor

    #         # Notify the successor about this node
    #         response = requests.post(f"http://{self.successor}/update-predecessor", json={'predecessor': self.address}, timeout=5)
    #         response.raise_for_status()

    #         print(f"Stabilization complete for node {self.address}. Successor is {self.successor}", flush=True)

    #     except requests.exceptions.RequestException as e:
    #         # If successor is unresponsive (e.g., crashed), update successor to its next available node
    #         print(f"Error stabilizing: {e}. Assuming successor {self.successor} is down.", flush=True)

    #         # Here we bypass the crashed node (current successor) and find the next live node
    #         try:
    #             # Contact the successor's successor
    #             response = requests.get(f"http://{self.successor}/successor", timeout=5)
    #             response.raise_for_status()
    #             new_successor = response.json()['successor']

    #             # Update this node's successor
    #             self.successor = new_successor

    #             # Notify the new successor that this node is now its predecessor
    #             response = requests.post(f"http://{self.successor}/update-predecessor", json={'predecessor': self.address}, timeout=5)
    #             response.raise_for_status()

    #             print(f"Updated successor for node {self.address} to {self.successor} after detecting crash.", flush=True)
            
    #         except requests.exceptions.RequestException as e2:
    #             print(f"Error contacting next successor: {e2}. The network may be disconnected.", flush=True)


    def stabilize(self):
        if self.crashed:
            return "Node is crashed and cannot stabilize", 500

        """Periodically checks the successor's predecessor and updates if needed."""
        try:
            response = requests.get(f"http://{self.successor}/predecessor", timeout=5)
            response.raise_for_status()
            successor_predecessor = response.json()['predecessor']

            if successor_predecessor and hash_value(successor_predecessor) > hash_value(self.address) and hash_value(successor_predecessor) < hash_value(self.successor):
                self.successor = successor_predecessor

            response = requests.get(f"http://{self.successor}/successor-list", timeout=5)
            response.raise_for_status()
            successor_successor_list = response.json()['successor_list']
            self.successor_list = [self.successor] + successor_successor_list[:-1]  # Update our successor list

            response = requests.post(f"http://{self.successor}/update-predecessor", json={'predecessor': self.address}, timeout=5)
            response.raise_for_status()

            self.update_finger_table()

            print(f"Stabilization complete for node {self.address}. Successor is {self.successor}", flush=True)

        except requests.exceptions.RequestException as e:
            print(f"Error stabilizing: {e}. Assuming successor {self.successor} is down.", flush=True)
            self.handle_successor_failure()

    def handle_successor_failure(self):
        """Handle the case when the current successor is unresponsive."""
        # Try to find the next live node from the successor list
        for successor in self.successor_list[1:]: 
            try:
                response = requests.get(f"http://{successor}/node-info", timeout=5)
                response.raise_for_status()
                self.successor = successor
                print(f"Updated successor for node {self.address} to {self.successor} after detecting crash.", flush=True)

                response = requests.post(f"http://{self.successor}/update-predecessor", json={'predecessor': self.address}, timeout=5)
                response.raise_for_status()

                self.update_successor_list()
                return
            except requests.exceptions.RequestException:
                continue 

        print(f"All successors in the list are unresponsive for node {self.address}.", flush=True)

    def update_successor_list(self):
        """Update the successor list by contacting the current successor."""
        try:
            response = requests.get(f"http://{self.successor}/successor-list", timeout=5)
            response.raise_for_status()
            successor_successor_list = response.json()['successor_list']
            self.successor_list = [self.successor] + successor_successor_list[:-1]
            print(f"Updated successor list for node {self.address}: {self.successor_list}", flush=True)
        except requests.exceptions.RequestException as e:
            print(f"Failed to update successor list for node {self.address}: {e}", flush=True)

    def get_successor_list(self):
        if self.crashed:
            return jsonify({'error': 'Node is crashed and cannot get successor list'}), 500

        return jsonify({'successor_list': self.successor_list}), 200


    def find_successor(self, key_hash, start_node=None):
        if self.crashed:
            return "Node is crashed and cannot find a successor", 500

        """Find the successor of the given key hash."""
        if start_node is None:
            start_node = self.address

        try:
            response = requests.get(f"http://{start_node}/node-info", timeout=5)  # Add a timeout
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

        except requests.exceptions.RequestException as e:
            print(f"Error in find_successor: {e}. Assuming node {start_node} is down.", flush=True)
            # Try to bypass the unresponsive node and find the next available node
            try:
                response = requests.get(f"http://{self.successor}/successor", timeout=5)
                response.raise_for_status()
                return response.json()['successor']
            except requests.exceptions.RequestException as e2:
                print(f"Error contacting next node: {e2}.", flush=True)
            return None



    def find_closest_preceding_node(self, key_hash, node_info):
        """Find the closest preceding node in the finger table for a given key hash."""
        for finger in reversed(node_info['finger_table']):
            if hash_value(node_info['address']) < hash_value(finger) < key_hash:
                return finger
        return node_info['address']

    def update_finger_table(self):
        if self.crashed:
            return "Node is crashed and cannot update the finger table", 500

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
        if self.crashed:
            return "Node is crashed and cannot accept PUT requests", 500

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
        if self.crashed:
            return "Node is crashed and cannot accept GET requests", 500

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
    if node1.crashed:
        return jsonify({'error': 'Node is crashed and cannot join the network'}), 500

    nprime = request.args.get('nprime')
    if nprime:
        node1.join(nprime)
        return jsonify({'message': f'Joined network through {nprime}'}), 200
    else:
        return jsonify({'error': 'No nprime specified'}), 400


@app.route('/leave', methods=['POST'])
def leave_network():
    if node1.crashed:
        return jsonify({'error': 'Node is crashed and cannot leave the network'}), 500

    node1.leave()
    return jsonify({'message': 'Node has left the network'}), 200

# Simulate a node crash
@app.route('/sim-crash', methods=['POST'])
def simulate_crash():
    node1.crashed = True
    print(f"Node {node1.address} has crashed", flush=True)
    return jsonify({'message': 'Node has crashed'}), 200

# Simulate a node recovery
@app.route('/sim-recover', methods=['POST'])
def simulate_recovery():
    node1.crashed = False
    print(f"Node {node1.address} has recovered", flush=True)

    if node1.successor != node1.address: 
        try:
            print(f"Attempting to rejoin the network through previous successor {node1.successor}", flush=True)

            node1.successor = node1.find_successor(node1.node_id, node1.successor)

            response = requests.get(f"http://{node1.successor}/predecessor")
            response.raise_for_status()
            node1.predecessor = response.json()['predecessor']

            if node1.successor:
                response = requests.post(f"http://{node1.successor}/update-predecessor", json={'predecessor': node1.address})
                response.raise_for_status()

            if node1.predecessor:
                response = requests.post(f"http://{node1.predecessor}/update-successor", json={'successor': node1.address})
                response.raise_for_status()

            node1.stabilize()
            node1.update_finger_table()
            node1.update_successor_list()

            print(f"Rejoined the network successfully through {node1.successor}", flush=True)

        except requests.exceptions.RequestException as e:
            print(f"Failed to rejoin the network through {node1.successor}: {e}", flush=True)

        node1.stabilize()
    else:
        node1.predecessor = None
        node1.successor_list = [node1.address] * len(node1.successor_list)

    return jsonify({'message': 'Node has recovered and attempted to rejoin the network'}), 200

@app.route('/node-info', methods=['GET'])
def get_node_info():
    if node1.crashed:
        return jsonify({'error': 'Node is crashed and cannot provide info'}), 500

    others = [node for node in node1.finger_table if node != node1.successor]

    return jsonify({
        'address': node1.address,
        'node_hash': node1.node_id,
        'successor': node1.successor,
        'predecessor': node1.predecessor,
        'finger_table': node1.finger_table,
        'others': others,
        'successor_list': node1.successor_list    
    }), 200

@app.route('/successor-list', methods=['GET'])
def get_successor_list():
    return node1.get_successor_list()

@app.route('/update-predecessor', methods=['POST'])
def update_predecessor():
    if node1.crashed:
        return jsonify({'error': 'Node is crashed and cannot update predecessor'}), 500

    new_predecessor = request.json['predecessor']
    node1.predecessor = new_predecessor
    return jsonify({'message': 'Predecessor updated'}), 200

@app.route('/update-successor', methods=['POST'])
def update_successor():
    if node1.crashed:
        return jsonify({'error': 'Node is crashed and cannot update successor'}), 500

    new_successor = request.json['successor']
    node1.successor = new_successor
    return jsonify({'message': 'Successor updated'}), 200

@app.route('/predecessor', methods=['GET'])
def get_predecessor():
    if node1.crashed:
        return jsonify({'error': 'Node is crashed and cannot get predecessor'}), 500

    return jsonify({'predecessor': node1.predecessor}), 200

@app.route('/successor', methods=['GET'])
def get_successor():
    if node1.crashed:
        return jsonify({'error': 'Node is crashed and cannot get successor'}), 500

    return jsonify({'successor': node1.successor}), 200

@app.route('/storage/<key>', methods=['PUT'])
def put_value(key):
    if node1.crashed:
        return jsonify({'error': 'Node is crashed and cannot store values'}), 500

    value = request.data.decode('utf-8')
    response = node1.put(key, value)
    return Response(response, content_type='text/plain'), 200

@app.route('/storage/<key>', methods=['GET'])
def get_value(key):
    if node1.crashed:
        return jsonify({'error': 'Node is crashed and cannot retrieve values'}), 500

    value = node1.get(key)
    if value is not None:
        return Response(value, content_type='text/plain'), 200
    else:
        return Response("Key not found", content_type='text/plain'), 404

@app.route('/fingertable', methods=['GET'])
def get_finger_table():
    if node1.crashed:
        return jsonify({'error': 'Node is crashed and cannot get finger table'}), 500

    return jsonify({'fingertable': node1.finger_table}), 200

@app.route('/helloworld', methods=['GET'])
def helloworld():
    if node1.crashed:
        return jsonify({'error': 'Node is crashed and cannot respond to requests'}), 500

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
            if not node1.crashed:
                node1.stabilize()
            time.sleep(10)  # Run stabilize every 10 seconds

    # Start stabilization in a background thread
    thread = threading.Thread(target=stabilization_task)
    thread.daemon = True  # Daemon thread exits when the main program exits
    thread.start()

    # Start the Flask server
    app.run(host="0.0.0.0", port=port)