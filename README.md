# mHGN Spark
mHGN: Multidimensional Heirarchical Graph Neuron is a pattern recognition scheme that uses parallel network centric processing at its core. 
This repository is about training mHGN network over Apache Spark cluster for recognizing multidimensional patterns in data streams.

## Spark Cluster Setup
Create a Spark cluster with atleast two worker instances and add Spark_Cluster directory in user directory of all the machines in the cluster.

## Training mHGN
To train mHGN, place your training dataset in training_files/training_patterns directory of all the nodes as driver program will be deployed on any of the node in the cluster. Next, from master node start all the daemons for Spark with `$ start-all.sh`
This will launch master node and all the worker nodes in the cluster. Check the status of all the workers with `jps` command.

Once all the nodes have been launched in Spark cluster, submit a job to Spark. This job will execute following operations:
1. Read training dataset from training_files directory into RDD with 12 partitions.
2. Distribute these partitions to all the worker nodes equally.
3. Merge all the partitions on each node into one training dataset.
4. Launch mHGN on each node and train it with respective training files.
5. Transfer the state files between nodes using `scp`.

The command to submit a job to Spark is as follows:
  
  
  ` $ spark-submit --master spark://192.168.0.26:7077  
            --deploy-mode cluster  
            --class spark_distribute_training_file  
            train-slgn_2.11-1.0.jar  
            192.168.0.25  
            1`    

Where,  
-- master < URL of master node >  
-- deploy-mode < cluster or client >  
-- class < Driver program >   
driver_program_jar  
node_to_train  
dataset_id
