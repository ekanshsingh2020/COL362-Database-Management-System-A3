# Assignment 3 : B+ Tree and Index Scan
### Goal 
- To get used to Physical storage structures of database file on disk, the assignment tried to mimic this by storing it in memory
- Indexing database files using B+ trees, implementing various all the functionalities from scratch
- Implementing a new operator using Apache Calcite and then use it to evaluate simple queries
- The instructions to run are provided in the assignemnt pdf, follow them strictly else you would face issues

### Idea Overview
#### B+Tree Implementation
- I will mostly touch upon the sequence in which I approached the functionalities so that it was easier to debug and also some important points
- I started with StorageManager.java as it had functions in the lowest level in the execution sequence and hence it was easier to debug it and then move up the ladder in execution
- After this I implemented the functionalities in TreeNode.java and LeafNode.java because the functions here called the functions defined in StorageManager.java which have already been implemented
- Now that we have all the operations setup for the LeafNode and TreeNode, I started with InternalNode.java as all the operations here would be used next when while inserting we face that the leafnodes are full and then we form internal nodes after splitting the leafnode into two
- Now we move towards BPlusTreeIndexFile.java which is the most important file as this is the first point of execution from the test files to create indexes from StorageManager and inserting elements from the records
- Here we implement the actual search and insert logic inside the insert function which uses the functionalities of LeafNodes and InternalNodes
- We start key insertion from the appropriate leaf node by searching the same and then in case of a full leafnode we start moving upwards in the tree and keep splitting nodes until we get a valid configuration
- The codebase is completly generic so as to handle any type of key datatypes and not just integer types which posed a lot of issues while writing the same
- This ended with the B+ Tree implementation

#### PIndexScan
- Now we had to implement Index Scan operator using Apache Calcite
- The rules were already written for us, we had to implement the evaluate function which took input as the storagemanager object
- As you can see the code seems pretty bad and repetitive, this is because we has cover all the possible operations during indexing and all the possible data types as well
- This can also be done in a far more better which I could not implement in this assignment to time crunch but I have implemented the same in the next assignment i.e. Assignment 4

This assignment for me was one of the toughest assignments due to the huge codebase and the amount of complexities involed in the same.

Let me know if you have any doubts regarding any part of the code :)