import time
import requests
import sys
import matplotlib.pyplot as plt
import statistics


def perform_put_requests(node_address, num_operations):
    keys = []
    # Start timer
    start_time = time.time()
    
    # Loop over the number of operations
    for i in range(num_operations):
        key = f'key-{i}'
        value = f'value-{i}'
        # PUT request
        response = requests.put(f"http://{node_address}/storage/{key}", data=value)
        if response.status_code != 200:
            print(f"Failed PUT request for key: {key}")
        keys.append(key)
    
    # Calculate time
    elapsed_time = time.time() - start_time
    
    return elapsed_time, keys

def perform_get_requests(node_address, keys):
    # Start timer
    start_time = time.time()
    # Loop over all the keys from 'perform_put_requests'
    for key in keys:
        # GET request
        response = requests.get(f"http://{node_address}/storage/{key}")
        if response.status_code != 200:
            print(f"Failed GET request for key: {key}")
    # Calculate time
    elapsed_time = time.time() - start_time
    return elapsed_time

def run_experiment(node_addresses, num_operations):
    
    # initialize PUT and GET times
    total_put_time = 0
    total_get_time = 0

    # Loop over all the nodes
    for node_address in node_addresses:
        print(f"Measuring PUT time for node {node_address}...")
        # Perform PUT requests
        put_time, keys = perform_put_requests(node_address, num_operations)
        # add time to the total time
        total_put_time += put_time

        print(f"Measuring GET time for node {node_address}...")
        # perform GET requests
        get_time = perform_get_requests(node_address, keys)
        # add time to the total time
        total_get_time += get_time

    # Calculate the average times
    avg_put_time = total_put_time / len(node_addresses)
    avg_get_time = total_get_time / len(node_addresses)

    print(f"Average PUT Time: {avg_put_time} seconds")
    print(f"Average GET Time: {avg_get_time} seconds")

    return avg_put_time, avg_get_time

def run_trials(node_addresses, num_operations, num_trials):
    """
    Run the experiment multiple times to calculate mean and standard deviation.
    """
    put_times = []
    get_times = []

    for _ in range(num_trials):
        put_time, get_time = run_experiment(node_addresses, num_operations)
        put_times.append(put_time)
        get_times.append(get_time)

    # calculatin the mean and standard deviation
    mean_put_time = statistics.mean(put_times)
    mean_get_time = statistics.mean(get_times)
    stdev_put_time = statistics.stdev(put_times)
    stdev_get_time = statistics.stdev(get_times)

    return mean_put_time, mean_get_time, stdev_put_time, stdev_get_time

def plot_results(node_counts, put_times, get_times, put_stdevs, get_stdevs):
    """Plots the elapsed time for PUT and GET operations vs. the number of nodes with error bars."""
    plt.errorbar(node_counts, put_times, yerr=put_stdevs, fmt='r-o', label='PUT Time (s)', capsize=5)
    plt.errorbar(node_counts, get_times, yerr=get_stdevs, fmt='b-o', label='GET Time (s)', capsize=5)
    plt.xlabel("Number of Nodes")
    plt.ylabel("Time (seconds)")
    plt.title("PUT/GET Time vs. Number of Nodes")
    plt.legend()
    plt.grid(True)
    plt.savefig("time_vs_nodes_plot.png")
    plt.show()
    print("Plot saved as time_vs_nodes_plot.png")


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python experiment.py <node1> <node2> ...")
        sys.exit(1)

    node_addresses = sys.argv[1:-1]
    num_operations = 10
    num_trials = 3  # Run 3 trials for each node count to calculate standard deviation

    print(f"Running experiment with nodes: {node_addresses} and {num_operations} operations.")

    # Run the experiment for x number of nodes (1, 2, 4, 8, 16)
    node_counts = [1, 2, 4, 8, 16]
    
    # Initializing PUT and GET times and standard deviations
    put_times = []
    get_times = []
    put_stdevs = []
    get_stdevs = []

    for node_count in node_counts:
        print(f"Testing with {node_count} node(s)...")
        selected_nodes = node_addresses[:node_count]  # Select the number of nodes
        mean_put_time, mean_get_time, stdev_put_time, stdev_get_time = run_trials(selected_nodes, num_operations, num_trials)
        put_times.append(mean_put_time)
        get_times.append(mean_get_time)
        put_stdevs.append(stdev_put_time)
        get_stdevs.append(stdev_get_time)

    # potting the results with Matplotlib with including error bars
    plot_results(node_counts, put_times, get_times, put_stdevs, get_stdevs)
