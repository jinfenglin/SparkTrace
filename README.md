SparkTrace
===
A framework which can create a traceability models on Spark.


###TODO
1. Replace the native pipeline (as SDF,DDF part of the workflow) with customized pipeline. Allow user to construct a pipeline without Spark knowledge
1. <del>Resolve the name conflicts in the dataframe, this could happen when csv have duplicated name or joining two dataset together.</del>
1. <del>Feature selection which define the input and output of a pipeline. It can reduce the size(dimension) of datasize and reduce the chance of key/column duplication</del>
1. <del>NullPointException for tokenizer if a column is empty. (Will Not Fix)</del>
