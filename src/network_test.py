import requests
import sys
import time
import json

def join_network(nodes):
    """Join nodes to form a ring network"""
    if len(nodes) < 2:
        print("Need at least two nodes to form a network.")
        return

    for i in range(1, len(nodes)):
        node_to_join = nodes[i]
        nprime = nodes[0]  # Join with the first node
        try:
            print(f"Node {node_to_join} is joining the network via {nprime}...")
            response = requests.post(f"http://{node_to_join}/join?nprime={nprime}")
            if response.status_code == 200:
                print(f"Node {node_to_join} successfully joined the network.")
            else:
                print(f"Failed to join node {node_to_join}. Status Code: {response.status_code}")
        except Exception as e:
            print(f"Error joining node {node_to_join}: {str(e)}")
        time.sleep(1)  # Adding delay between join calls

def leave_network(nodes):
    """Make each node leave the network one by one"""
    for node in nodes:
        try:
            print(f"Node {node} is leaving the network...")
            response = requests.post(f"http://{node}/leave")
            if response.status_code == 200:
                print(f"Node {node} successfully left the network.")
            else:
                print(f"Failed to leave node {node}. Status Code: {response.status_code}")
        except Exception as e:
            print(f"Error when node {node} tried to leave: {str(e)}")
        time.sleep(2)  # Adding delay between leave calls


def simulate_crash_and_recovery(nodes):
    """Simulate crash and recovery of nodes"""
    for node in nodes:
        try:
            # Simulate crash
            print(f"Simulating crash for node {node}...")
            crash_response = requests.post(f"http://{node}/sim-crash")
            if crash_response.status_code == 200:
                print(f"Node {node} simulated a crash successfully.")
            else:
                print(f"Failed to crash node {node}. Status Code: {crash_response.status_code}")
            
            time.sleep(5)  # Simulate downtime before recovery

            # Simulate recovery
            print(f"Simulating recovery for node {node}...")
            recover_response = requests.post(f"http://{node}/sim-recover")
            if recover_response.status_code == 200:
                print(f"Node {node} recovered successfully.")
            else:
                print(f"Failed to recover node {node}. Status Code: {recover_response.status_code}")
        except Exception as e:
            print(f"Error simulating crash/recovery for node {node}: {str(e)}")
        time.sleep(2)

def main():
    if len(sys.argv) != 2:
        print("Usage: networks_tests.py '[\"node1\", \"node2\", \"node3\"]'")
        sys.exit(1)
    try:
        # Parse the argument as a JSON list
        nodes = json.loads(sys.argv[1])
    except json.JSONDecodeError:
        print("Error: The argument should be a valid JSON list of nodes.")
        sys.exit(1)

    if not isinstance(nodes, list):
        print("Error: The argument should be a JSON array.")
        sys.exit(1)

    print(f"Testing network with nodes: {nodes}")

    # Perform network operations
    join_network(nodes)

    # Optionally run other tests
    # Uncomment to test leave network
    # leave_network(nodes)

    # Uncomment to test crash and recovery
    # simulate_crash_and_recovery(nodes)


if __name__ == "__main__":
    main()