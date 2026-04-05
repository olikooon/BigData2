## app folder
This folder contains the data folder and all scripts and source code that are required to run your simple search engine. 

### data
This folder stores the text documents required to index. Here you can find a sample of 100 documents from `a.parquet` file from the original source.

### mapreduce
This folder stores the mapper `mapperx.py` and reducer `reducerx.py` scripts for the MapReduce pipelines.

### app.py
This is a Python file to write code to store index data in Cassandra.

### app.sh
The entrypoint for the executables in your repository and includes all commands that will run your programs in this folder.

### create_index.sh
A script to create index data using MapReduce pipelines and store them in HDFS.

### index.sh
A script to run the MapReduce pipelines and the programs to store data in Cassandra/ScyllaDB.

### prepare_data.py
The script that will create documents from parquet file. You can run it in the driver.

### prepare_data.sh
The script that will run the prevoious Python file and will copy the data to HDFS.

### query.py
A Python file to write PySpark app that will process a user's query and retrieves a list of top 10 relevant documents ranked using BM25.

### requirements.txt
This file contains all Python depenedencies that are needed for running the programs in this repository. This file is read by pip when installing the dependencies in `app.sh` script.

### search.sh
This script will be responsible for running the `query.py` PySpark app on Hadoop YARN cluster.


### start-services.sh
This script will initiate the services required to run Hadoop components. This script is called in `app.sh` file.


### store_index.sh
This script will create Cassandra/ScyllaDB tables and load the index data from HDFS to them.

### rebuild_input_from_hdfs.py
PySpark job: rebuilds **`/input/data`** (one partition) from all **`/data/*.txt`**. Used by **`add_to_index.sh`**.

### add_to_index.sh
Optional (+10): upload one local **`<id>_<title>.txt`** to HDFS **`/data`**, rebuild **`/input/data`**, run **`index.sh`** (full reindex + Cassandra).
