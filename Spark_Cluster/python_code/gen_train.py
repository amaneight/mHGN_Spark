#Created by Aman Sehgal ; 20/4/2018  
 
# Generate training file for mHGN and train mHGN on Spark Cluster


import os
import sys
import logging

# Fetch partiton files distributed by Spark Context
def fetch_parts_files():
	l=[]

	#If partition belongs to node that doesn't host driver   
	if os.path.exists(worker_partition_path):
		print "Not a driver node"
	
		# Fetch all tasks
		a = os.walk(worker_partition_path).next()[1]

		# Remove _temporary from tasks
		a.pop(a.index('_temporary')) if '_temporary' in a else 1

		# Get all parts of each task
		b = [os.walk(worker_partition_path + task).next() for task in a]
		l = []
		for g in range(0, len(b)):
			b_path = b[g][0]
			part_files = b[g][2]
			p_files = []
			for m in range(0, len(part_files)):
				if part_files[m].find('.crc') < 0:
					p_files.append(part_files[m])
			for qw in p_files:
				l.append(b_path + '/' + qw)

	else:
		# If parition belongs to node that hosts driver
		print "Driver node"

		partition_path = "/home/user/Spark_Cluster/processing_files/processing_patterns/partition"
		files_at_partition_path = os.walk(partition_path).next()[2]

		for file in files_at_partition_path:
			if file.find('.crc') < 0 and file.find('SUCCESS') < 0:
				partition_file_name = partition_path + '/' + file
				l.append(partition_file_name)

	return l


def combine_train_files(part_files):
	# Combine all parts files into one trainining file
	with open(compiled_training_file, 'w') as outfile:
		for fname in part_files:
			with open(fname) as infile:
				outfile.write(infile.read())

	print "Training files combined"


def generate_train_file():
	# Convert flat representation of character to list and extract character
	# Eg:. A,XXXX,X -> ['A',',','X','X','X'.......'X']

	# Get all flat patterns from test file Eg: A,XXX,XX , BXXXX,XX,XX
	flat_patterns = []
	with open(compiled_training_file, 'r') as tr_file:
		flat_patterns = tr_file.readlines()

	with open(training_pattern_unix_format, "w") as f:
		for pattern in flat_patterns:
			# Extract character representation of patterns i.e A for A,XXX,XX
			curr_char = list(pattern)[0]
			list_char_rep = pattern.split(curr_char)[1].split('\n')[0]

			# with open("/home/user/op.txt", "a") as f:
			f.write('S' + '\n' + curr_char.upper() + '\n')
			# Convert linear pattern in 2D representation
			f.write('\n'.join(
				[''.join(z) for z in [list_char_rep[y - 15:y] for y in [x * 15 for x in range(1, end_range)]]]))
			f.write('\n')

	print "Training files generated"

# Convert unix file to dos file
def generate_train_file_dos_format():
	ps = os.popen("touch " + training_pattern_dos_format).read()
	ps = os.popen("unix2dos -n " + training_pattern_unix_format + " " + training_pattern_dos_format).read()

	print "Training files converted to DOS format"

# Get number of patterns
def get_no_of_patterns():
	with open(training_pattern_dos_format, 'r') as f:
		lines = f.readlines()
		if len(lines) > 0:
			return len(lines) / 17
	return -1


# Execute mHGN
def executeHGN():
	no_of_patterns = get_no_of_patterns()
	self_ip = get_ip()

	if no_of_patterns != -1:

		cmd = "java -jar " + slgn_jar + " 15,15 " + str(no_of_patterns) + " " + training_pattern_dos_format + " " +  slgn_result_file + " " + slgn_test_file + " " + processing_slgn_state_path
		print "Executing command: " + cmd

		ps = os.popen(cmd).read()


# Get ip of current machine
def get_ip():
	ps = os.popen(
		"ifconfig | grep -Eo 'inet (addr:)?([0-9]*\.){3}[0-9]*' | grep -Eo '([0-9]*\.){3}[0-9]*' | grep -v '127.0.0.1' | grep -v '172.17.0.1' ").read().strip(
		'\n')
	print "IP: " + ps
	return ps


# Transfer state files
def transfer_exact_files():
	self_ip = get_ip()

	
	ps = os.popen(
		"scp user@" + self_ip + ":" + processing_slgn_state_path + " user@" + train_node_ip + ":" + training_slgn_state_path ).read()

	print "State file transferred from " + self_ip + " to " + train_node_ip


# File paths
worker_partition_path = '/home/user/Spark_Cluster/processing_files/processing_patterns/partition/_temporary/0/'
driver_partition_path = '/home/user/Spark_Cluster/processing_files/processing_patterns/partition/'
compiled_training_file = '/home/user/Spark_Cluster/processing_files/processing_patterns/compiled_training_file.txt'
training_pattern_unix_format = '/home/user/Spark_Cluster/processing_files/processing_patterns/worker_training_data_unix.txt'
training_pattern_dos_format = '/home/user/Spark_Cluster/processing_files/processing_patterns/worker_training_data_dos.txt'
slgn_jar = '/home/user/Spark_Cluster/SLGN_files/SLGN.jar'
slgn_result_file = '/home/user/Spark_Cluster/SLGN_files/result'
slgn_test_file = '/home/user/Spark_Cluster/SLGN_files/test.txt'


# Check if arguments  were provided for train node ip and dataset id
if len(sys.argv) > 1:
	train_node_ip = sys.argv[1]
	dataset_id = sys.argv[2]

state_file_name = str(get_ip()) + '_' + dataset_id + '.state'

processing_slgn_state_path = '/home/user/Spark_Cluster/processing_files/processing_state_files/' + state_file_name

training_slgn_state_path = "/home/user/Spark_Cluster/training_files/training_state_files"

# Check if compiled training file exists
if os.path.exists(compiled_training_file) == False:
	print "Compiled SLGN state file name : " + compiled_training_file + " created"

	# Fetch and combine part files (distributed training files)
	parts_files = fetch_parts_files()
	combine_train_files(parts_files)
	
	dimension = 2
	second_dimension = 15
	second_dimension_multiplier = 1
	end_range = (second_dimension * second_dimension_multiplier) + 1

	# Compile training file 
	generate_train_file()
	print "Training file in unix format generated"

	# Convert unix format of file to dos format
	generate_train_file_dos_format()
	print "Training file in dos format generated"

	# Execute mHGN
	executeHGN()
	print "Executing HGN"

	# Transfer state file
	transfer_exact_files()
	"Transferring exact files"

else:
	print "Compiled SLGN state file name : " + compiled_training_file + " already exists"
