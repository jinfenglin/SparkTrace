SparkTrace
===
A framework which can create a traceability models on Spark.

Create a graph
===
To create a graph programmer need to do following steps:
1. Create an empty graph as background
2. Define the IO fields for the graph. If the graph is SDFGraph, then assign the type of output to output fields
3. Create SNodes.If the nodes are added into SDFGraph, then the pipeline stages should be wrapped in NullRemoverStage 
4. Add nodes to the graph
5. 

###TODO

TODO: 
3. Modify the SDFGraph which only need to receive sdftype at SDFGraph level and can convert a graph into SDFGraph

Experiments:
(Voting system is the best example for demonstration. But in paper,we need to explain other usage cases)
1. Voting system (VSM + LDA + VSM_NGRam) with and without optimization 
2. Voting system with and without infusion
3. Voting system cache point experiment
3. ICSE example 
