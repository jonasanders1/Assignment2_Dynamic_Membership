import sys
import json
import requests
import time
import statistics
import matplotlib.pyplot as plt


TRIALS = 3  # the number of trials 
SIZES = [2, 4, 8, 16, 32]  # the network sizes

# function that:
# --> Starts timer
#   --> Loops over all the nodes and joins them together via the first node in the list
#     --> Returns the nodes and the current time
def join_nodes(nodes):
    if len(nodes) < 2:
        raise ValueError("Need at least two nodes to form a network.")
    
    base_node = nodes[0] # nprime node
    print(f"Base node for joining: {base_node}")
    start_time = time.time()  # starting timer

    for i in range(1, len(nodes)):
        node_to_join = nodes[i]
        print(f"Joining node {node_to_join} to the network via {base_node}...")
        
        try:
            response = requests.post(f"http://{node_to_join}/join?nprime={base_node}")
            if response.status_code == 200:
                print(f"Node {node_to_join} successfully joined.")
            else:
                print(f"Failed to join node {node_to_join}. Status Code: {response.status_code}")
        except Exception as e:
            print(f"Error joining node {node_to_join}: {str(e)}")
        time.sleep(1)  # a small delay between joins
    return wait_for_stabilization(nodes, start_time)


# function that:
# --> loops over all the nodes and requests their successor and predecessor
#   --> if successor or predecessor are 'None' --> Not done stabilizing
#     --> measuring the total time when stabilized
def wait_for_stabilization(nodes, start_time):
    stabilized = False
    while not stabilized:
        stabilized = True
        for node in nodes:
            try:
                response = requests.get(f"http://{node}/node-info")
                response.raise_for_status()
                node_info = response.json()
                # Check if the node has valid successor and predecessor (customize this to your needs)
                if node_info['successor'] is None or node_info['predecessor'] is None:
                    stabilized = False
                    break
            except Exception as e:
                print(f"Error probing node {node}: {str(e)}")
                stabilized = False
                break
        time.sleep(1)  # Poll every second
    
    # Once stabilized, return the time taken
    end_time = time.time()
    return end_time - start_time


# function that:
# --> loops over different network sizes
#   --> for each size, runs three times
#     --> in each trial:
#       --> joins the nodes into a network and measures the time it takes for the network to stabilize
#       --> stores the time taken for each trial
#     --> calculates the average and standard deviation of the join times for the given network size
# --> returns a dictionary containing the mean and standard deviation of join times for each network size
def run_experiment(nodes_list, sizes, trials):
    results = {}
    for size in sizes:
        nodes = nodes_list[:size]
        trial_times = []
        print(f"\n=== Running experiment for {size} nodes ===\n")
        for trial in range(trials):
            print(f"Trial {trial+1} for {size} nodes...")
            trial_time = join_nodes(nodes)
            trial_times.append(trial_time)
            print(f"Time taken for Trial {trial+1}: {trial_time:.2f} seconds")
        
        mean_time = statistics.mean(trial_times)
        std_dev = statistics.stdev(trial_times) if len(trial_times) > 1 else 0.0
        results[size] = {
          'mean': mean_time, 
          'std_dev': std_dev
        }
        
        print(f"\nAverage time for {size} nodes: {mean_time:.2f} seconds (std: {std_dev:.2f})\n")

    return results


# function to plot the results
def plot_results(results):
    """Plot the results with error bars."""
    sizes = list(results.keys())
    means = [results[size]['mean'] for size in sizes]
    std_devs = [results[size]['std_dev'] for size in sizes]

    plt.errorbar(sizes, means, yerr=std_devs, fmt='-o', capsize=5, ecolor='red', label='Time to Stabilize')
    plt.title('Network Stabilization Time vs. Number of Nodes')
    plt.xlabel('Number of Nodes')
    plt.ylabel('Time to Stabilize (seconds)')
    plt.xticks(sizes)
    plt.grid(True)
    plt.legend()
    
    plt.savefig('network_stabilization_plot.png')
    print("Plot saved as 'join_network_experiment.png'")

def main():
    if len(sys.argv) != 2:
        print("Usage: python network_experiment.py '[\"node1\", \"node2\", ...]'")
        sys.exit(1)
    try:
        # Parse nodes list
        nodes_list = json.loads(sys.argv[1])
    except json.JSONDecodeError:
        print("Error: The argument should be a valid JSON list of nodes.")
        sys.exit(1)
    if not isinstance(nodes_list, list) or len(nodes_list) < max(SIZES):
        print(f"Error: You need at least {max(SIZES)} nodes to run the experiment.")
        sys.exit(1)

    # run the experiment
    results = run_experiment(nodes_list, SIZES, TRIALS)

    # plot the results
    plot_results(results)


if __name__ == "__main__":
    main()
