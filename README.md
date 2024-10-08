# Cluster Comands

### Start cluster server
```ssh joand4770@ificluster.ifi.uit.no```
### List nodes available right now
```/share/ifi/available-nodes.sh```
### List the cluster hardware
```/share/ifi/list-cluster-static.sh```
List nodes by load
```/share/ifi/list-nodes-by-load.sh```
### List all your processes
```/share/ifi/list-cluster-my-processes.sh```
### Clean out all your processes
```/share/ifi/cleanup.sh```

### List all the nodes in the cluster along with their hardware details
```/share/ifi/list-cluster-static.sh```
### Shows witch nodes currently available
```/share/ifi/available-nodes.sh```
### This will display the name of the current node you are logged into
```echo $HOSTNAME```

## 1. Navigate to node and create project
  - List all nodes and navigate to an available node.
    - ```ssh cXX-XX```
  - Verify which node im on:
    - ```echo $HOSTNAMe —> cXX-XX.ifi.uit.no```
  - Create a project directory
    - ```mkdir joand4770```
    - ```cd joand4770```

## 2. Connect to the Cluster Using VS Code:
  - Open VS Code.
  - Press  ```Cmd + Shift + P``` to open the Command Palette.
  - Type Remote-SSH: Connect to Host... and select it.
  - Enter your cluster’s SSH address (e.g., your_username@ificluster.ifi.uit.no) and your password.

## 3. Open project in vs code within the cluster:
  - run the python server script
  - ```python server.py (python3 -m http.server 5000)```
  -  --—> Server running on ificluster.ifi.uit.no:37059
  - ```curl http://ificluster.ifi.uit.no:44993```

## 4.Run run shell script
  - ```bash run.sh 3```
  - --—> Successfully updated c7-23:59064
  - ---> [ "c6-5:6258", "c6-4:54341", "c11-0:15361", "c7-23:59064" ]

## 5. Run ```run-tester.py```
  — ```python run-tester.py '[ "c6-5:6258", "c6-4:54341", "c11-0:15361", "c7-23:59064" ]'```
  received "c6-5:6258"
  received "c6-4:54341"
  received "c11-0:15361"
  received "c7-23:59064"
  Success!

## Terminal commands for Putting a value and Getting a value based on keys
### PUT 
```curl -X PUT -H "Content-Type: text/plain" -d 'value1' http://172.21.21.222:10468/storage/testkey1'```
### GET 
```curl  http://172.21.21.222:10468/storage/testkey1```
### GET Network
```curl http://172.21.21.222:10468/network```