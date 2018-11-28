# MongoDB
Python scripts for practicing with mongoDB

Python version: 3.6.3

MongoDB version: 3.6

The dataset used in these scripts where retreived from the GroupLens website (Univeristy of Minnesota). 
For any further information please visit https://grouplens.org/datasets/.

They are useful scripts that anybody startitng to learn how to use MongoDB can use as an inspiration. 
Specifically, they make exstensive use of the MongoDB Aggregation Pipeline. 

The project is structured as follows:
- data: contains the MovieLens dataset, consisting in 4 .csv files.
- constants: a .py containing constant declarations used throughout the whole project.
- batch_operations: these are scripts which mainly contain CRUD operations that populate 
the MongoDB Database and to structure the dataset inside it. They are inteded to run as batch operations. 
The batch_operations.py script executes the db population and adds calculated nested fields, using methods implemented
in the batch_operations_library.py script. It also performs a query to the "movies" collection, which selects
all the movies that have "love" as a tag. 
The batch_operations_log.txt file contains the results of the 
different operations performed in the batch_operations.py script in a "success/failed" format. 
The batch_operations_results directory contains the printed version of the collections created (to a maximum of 
10 records per collection), and the query's result.  

![alt text](https://drive.google.com/open?id=1EGsLVw8BJG2p1_sr-ZBKYQpN-WQXyv3S)
  

