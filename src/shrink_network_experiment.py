import requests
import sys
import time
import json
import matplotlib.pyplot as plt
import numpy as np


TRIALS = 3  # the number of trials 
SIZES = [2, 4, 8, 16, 32]  # the network sizes

def leave_network(nodes):
    """Make each node leave the network one by one."""
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
        time.sleep(1)  # Adding delay between leave calls to allow stabilization

def measure_shrink_time(nodes, target_size):
    """Measure the time to shrink the network from its current size to target size."""
    num_nodes = len(nodes)
    nodes_to_leave = num_nodes - target_size

    if nodes_to_leave <= 0:
        print(f"Target size {target_size} is not smaller than current size.")
        return None

    print(f"Shrinking network from {num_nodes} to {target_size} nodes...")

    start_time = time.time()

    # Make nodes leave until the target size is reached
    leave_network(nodes[-nodes_to_leave:])

    end_time = time.time()
    elapsed_time = end_time - start_time

    print(f"Time taken to shrink to {target_size} nodes: {elapsed_time:.2f} seconds")
    return elapsed_time


def run_experiment(nodes, sizes, trails):
    results = {}
    for size in sizes:
        trail_times = []
        print(f"\n=== Shrinking experiment from {size} nodes ===")
        for trial in range(3):
            print(f"\nTrial {trial + 1} for shrinking from {size} nodes...")
            current_nodes = nodes[:size]
            target_size = size // 2
            shrink_time = measure_shrink_time(current_nodes, target_size)
            trail_times.append(shrink_time)
        
        # Compute mean and standard deviation
        mean_time = np.mean(trail_times)
        std_dev_time = np.std(trail_times)
        results[size] = {
            'mean': mean_time,
            'std_dev': std_dev_time
        }
        
        print(f"Average time for shrinking from {size} to {target_size} nodes: {mean_time:.2f} seconds (std: {std_dev_time:.2f})")

    return results


# function to plot the results
def plot_shrink_results(results):
    """Plot the results of the shrinking experiment."""
    sizes = list(results.keys())
    means = [results[size]['mean'] for size in sizes]
    std_devs = [results[size]['std_dev'] for size in sizes]

    plt.errorbar(sizes, means, yerr=std_devs, fmt='-o', capsize=5, ecolor='red', label='Time to Shrink')
    plt.title('Network Shrinking Time vs. Number of Nodes')
    plt.xlabel('Starting Number of Nodes')
    plt.ylabel('Time to Shrink (seconds)')
    plt.xticks(sizes)
    plt.grid(True)
    plt.legend()
    
    # Save the plot as an image
    plt.savefig('network_shrink_plot.png')
    print("Plot saved as 'network_shrink_plot.png'")


def main():
    if len(sys.argv) != 2:
        print("Usage: shrink_network_experiment.py '[\"node1\", \"node2\", \"node3\", ...]'")
        sys.exit(1)
    try:
        nodes = json.loads(sys.argv[1])
    except json.JSONDecodeError:
        print("Error: The argument should be a valid JSON list of nodes.")
        sys.exit(1)
    if not isinstance(nodes, list):
        print("Error: The argument should be a JSON array.")
        sys.exit(1)
    # run the experiment
    results = run_experiment(nodes, SIZES, TRAILS)

    # Plot and save the results
    plot_shrink_results(results)


if __name__ == "__main__":
    main()
