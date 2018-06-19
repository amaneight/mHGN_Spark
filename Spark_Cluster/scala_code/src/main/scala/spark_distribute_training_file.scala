// Created by Aman Sehgal ; 12/4/2018  
	  
// Driver program for Spark Cluster  

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.io._
import sys.process._

object spark_distribute_training_file{
	def main(args: Array[String]){

		// Fetch IP of node to be trained from arguments
		if (args.length == 0) {
				
			println("IP for node to be trained is missing. Aborting training process.")

		} else {
	
			val training_node = args(0)
			val pattern_id = args(1)
	
			// Set app name
			val sc = new SparkContext(new SparkConf().setAppName("Spark Split File"))

			// Broadcast command to run python script to all worker nodes
			val pm = sc.broadcast("python /home/user/Spark_Cluster/python_code/gen_train.py " + training_node + " " + pattern_id)
		
			// Read training file
			val rdd = sc.textFile("/home/user/Spark_Cluster/training_files/training_patterns/train.txt",12)

			rdd.repartition(12)

			// Save partitions on respective worker nodes
			rdd.saveAsTextFile("/home/user/Spark_Cluster/processing_files/processing_patterns/partition/")

			// Execute python script on all worker nodes
			rdd.map(x=>pm.value!).collect()
		}
	}
}



