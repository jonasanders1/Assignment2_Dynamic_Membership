import sys
import json
import requests
import time

TRIALS = 1  # Number of trials for each burst size

def crash_nodes(nodes, num_crashes):
    """Simulate crashes for a specified number of nodes."""
    crashed_nodes = []
    for i in range(num_crashes):
        node = nodes[i]
        try:
            print(f"Crashing node {node}...")
            response = requests.post(f"http://{node}/sim-crash")
            if response.status_code == 200:
                print(f"Node {node} successfully crashed.")
                crashed_nodes.append(node)
            else:
                print(f"Failed to crash node {node}. Status Code: {response.status_code}")
        except Exception as e:
            print(f"Error crashing node {node}: {str(e)}")
        time.sleep(1)  # A small delay between crash requests
    return crashed_nodes

def recover_nodes(nodes):
    """Simulate recovery for a list of crashed nodes."""
    for node in nodes:
        try:
            print(f"Recovering node {node}...")
            response = requests.post(f"http://{node}/sim-recover")
            if response.status_code == 200:
                print(f"Node {node} successfully recovered.")
            else:
                print(f"Failed to recover node {node}. Status Code: {response.status_code}")
        except Exception as e:
            print(f"Error recovering node {node}: {str(e)}")
        time.sleep(1)  # A small delay between recovery requests

def is_network_stable(active_nodes, retries=5, delay=20, recheck_delay=5):
    """Check if the network forms a stable ring, with retries to handle delayed updates."""
    previous_map_length = 0  # Track the length of the successor map from the previous attempt
    for attempt in range(retries):
        visited = set()
        current_node = active_nodes[0]
        successor_map = {}

        # Build a map of each active node's successor
        print(f"\nBuilding successor map (Attempt {attempt + 1}/{retries})...")
        problematic_nodes = []  # Track nodes with problematic successors
        for node in active_nodes:
            try:
                response = requests.get(f"http://{node}/successor")
                response.raise_for_status()
                successor = response.json()['successor']
                # Only add to the map if the successor is also an active node
                if successor in active_nodes:
                    successor_map[node] = successor
                else:
                    problematic_nodes.append((node, successor))  # Log problematic nodes
            except Exception as e:
                print(f"Error checking successor of {node}: {str(e)}")
                return False

        # Log the current state of the successor map for debugging
        map_length = len(successor_map)
        print(f"Successor map (length: {map_length}): {successor_map}")

        # Check if all successors are unique
        if len(set(successor_map.values())) == len(active_nodes):
            print("All successors are unique.")
        else:
            print("Some nodes share the same successor.")
            time.sleep(delay)
            continue  # Retry the check

        # If all active nodes have unique successors and the map length matches the expected number of nodes,
        # the network should be considered stable.
        if map_length == len(active_nodes):
            print("The network has a complete and unique successor map. Network is stable.")
            return True

        # Wait before the next retry
        print(f"Retrying stability check, attempt {attempt + 1}/{retries}")
        time.sleep(delay)

    # If retries are exhausted, consider the network unstable
    print("Network did not stabilize after all retries.")
    return False

def run_burst_experiments(nodes_list):
    """Run experiments with increasing burst sizes of node crashes."""
    results = {}
    burst_size = 1
    
    while burst_size <= len(nodes_list):
        print(f"\n=== Running experiment for burst size {burst_size} ===\n")
        
        # Crash the specified number of nodes
        crashed_nodes = crash_nodes(nodes_list, burst_size)
        active_nodes = [node for node in nodes_list if node not in crashed_nodes]

        # Wait for stabilization and check if the network is stable
        print("Checking if the network stabilizes...")
        stable = is_network_stable(active_nodes)

        if stable:
            print(f"Network is stable with {burst_size} nodes crashed.")
            # Recover the crashed nodes
            recover_nodes(crashed_nodes)

            # Add a delay to allow the recovered node(s) to stabilize
            print("Waiting 30 seconds to allow the recovered nodes to stabilize...")
            time.sleep(30)

            # Increase the burst size for the next iteration
            burst_size += 1
        else:
            print(f"Network could not stabilize with {burst_size} nodes crashed.")
            results['max_crash_tolerance'] = burst_size - 1
            break
    
    # If we reached the end of the list, record the maximum crash tolerance
    if burst_size > len(nodes_list):
        results['max_crash_tolerance'] = len(nodes_list)
    
    print(f"\nMaximum burst size of crashes the network can tolerate: {results['max_crash_tolerance']}")
    return results

def main():
    if len(sys.argv) != 2:
        print("Usage: python network_crash_experiment.py '[\"node1\", \"node2\", ...]'")
        sys.exit(1)
    try:
        # Parse nodes list
        nodes_list = json.loads(sys.argv[1])
    except json.JSONDecodeError:
        print("Error: The argument should be a valid JSON list of nodes.")
        sys.exit(1)
    if not isinstance(nodes_list, list) or len(nodes_list) < 4:
        print("Error: You need at least 4 nodes to run the experiment.")
        sys.exit(1)

    # Run the burst experiment
    results = run_burst_experiments(nodes_list)

    # Display results
    print(f"Results: {results}")

if __name__ == "__main__":
    main()
