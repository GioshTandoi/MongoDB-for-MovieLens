# MongoDB
Python version: 3.6.3

MongoDB version: 3.6

pymongo version: 3.6.1

This project has been realized as part of the final assignment of the course "Databases Design with 
state-of-the-art models" at the University of Bari, and consists in the design of a movie database. 

The dataset used in these scripts are retreived from the GroupLens website (Univeristy of Minnesota). 
For any further information please visit https://grouplens.org/datasets/.

They are useful scripts that anybody starting  to learn how to use MongoDB can use as an inspiration. 
Specifically, they make exstensive use of the MongoDB Aggregation Pipeline. 

The project is structured as follows:
1.  DATA: contains the MovieLens dataset, consisting in 4 .csv files.
2.  CONSTANTS: a .py containing constant declarations used throughout the whole project.
3.  BATCH_OPERATIONS: these are scripts which mainly contain CRUD operations that populate 
the MongoDB Database and to structure the dataset inside it. They are intended to run as batch operations. 
The batch_operations.py script executes the db population and adds calculated nested fields, using methods implemented
in the batch_operations_library.py script. It also performs a query to the "movies" collection, which selects
all the movies that have "love" as a tag. 
The batch_operations_log.txt file contains the results of the 
different operations performed in the batch_operations.py script in a "success/failed" format. 
The batch_operations_results directory contains the printed version of the collections created (to a maximum of 
10 records per collection), and the query's result.  
The diagram below pictures how the dataset has been structured at this stage. 

![alt text](https://github.com/GioshTandoi/MongoDB/blob/master/DBStructures.png)
  
4. QUERIES: this directory contains scripts that perform different queries on every collection
of the DBs created in the previous step. These queries make an extensive use of the MongoDB Aggregation Pipeline. 
The queries_execution.py executes the queries and prints the results in the .txt files contained in the 
queries_results directory, using methods implemented in the queries_library.py script. Each script 
contain a detailed description of the queries performed. 
5. ONLINE_OPERATIONS: this script contains some functions that can be called when 
inserting operations happen on-line. it contains detailed comments on how it works, and how 
it takes into account the links existing between the different collections. 

