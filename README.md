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

Create a TraceTask
===
1. Create SDF Graph and DDFGraph
2. init the Task
3. infuse the task
4. optimize the task
5. Run train and test

###TODO
TODO: 
FLayer and SLayer:
1. Rework the SymbolValueSyn system in SLayer to make the output name can be specified in configuration.
2. Complete the Fschema system 
3. Add code to parse the parameters of TraceLab nodes
4. Complete the function getNodeContent for FNodes to enable FGraph to reuse SGraph optimization algorithm.
5. Complete the workflow system for the FGraph

Note:
TraceLab component must be assigned with a prefix to indicate its type

    NFN:(NFNode or Native Flow Node.) E.g. JOIN operation  Refer NFNode class
    CFN:(CFNode Composite Flow Node.) Refer CFNode class
    FG:(FGraph  or Flow graph)
    SG:SGraph
    SN:SNode: