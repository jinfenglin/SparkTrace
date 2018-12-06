package core;

import core.pipelineOptimizer.SGraph;
import java.util.List;

/**
 *
 */
public class SparkTraceGraph extends SGraph {
    SGraph sourceSDFGraph, targetSDFGraph, ddfGraph, modelGraph;

    public SparkTraceGraph() {
    }

    /**
     * Building SDFGraph to by scanning the ddfGraph. The SDFGraph and DDFGraph are two separated graphs,
     * when the DDFGrpah will be enrolled to generate pipeline construction automatically in the nested Graphs,
     * the SDFGraph should be utilized by the top level SparkTraceJob only. Even the SDFGraph will not be utilized
     * by SparkTraceGraph directly, it is responsible to keep the recursive structure. In other words, for a
     * SparkTraceTask ST which contains several subST, in the ST's SDFGraph should contains the subST's SDGraph as nodes.
     */
    private void buildSDFGraph() {
//        List<SparkTraceGraph> nestedSparkTraceGraph = findNestedSparkTraceGraph(ddfGraph);
//        SparkTraceGraph lastAddedGraph = null;
//        for (SparkTraceGraph curTraceGraph : nestedSparkTraceGraph) {
//            sourceSDFGraph.addNode(curTraceGraph.sourceSDFGraph);
//            targetSDFGraph.addNode(curTraceGraph.targetSDFGraph);
//            if (lastAddedGraph != null) {
//                Map<Symbol, Symbol> sourceIOMap = new HashMap<>();
//                sourceIOMap.put(new Symbol(lastAddedGraph.sourceSDFGraph, "id"),new Symbol(curTraceGraph.sourceSDFGraph,"id"));
//                Map<Symbol, Symbol> targetIOMap = new HashMap<>();
//                targetIOMap.put(new Symbol(lastAddedGraph.targetSDFGraph, "id"), new Symbol(curTraceGraph.targetSDFGraph,"id"));
//                SEdge sourceEdge = new SEdge(lastAddedGraph.sourceSDFGraph, curTraceGraph.sourceSDFGraph, sourceIOMap);
//                SEdge targetEdge = new SEdge(lastAddedGraph.targetSDFGraph, curTraceGraph.targetSDFGraph, targetIOMap);
//                sourceSDFGraph.addEdge(sourceEdge);
//                targetSDFGraph.addEdge(targetEdge);
//            }
//            lastAddedGraph = curTraceGraph;
//        }
    }

    /**
     * Recursively find the SparkTraceGraph from the root graph. In the procedure,
     * if a SparkTraceGraph is found, the recursion will not go into the SparkTraceGraph
     *
     * @param rootGraph
     * @return
     */
    private List<SparkTraceGraph> findNestedSparkTraceGraph(SGraph rootGraph) {
//        List<SparkTraceGraph> foundGraph = new ArrayList<>();
//        List<SNode> nodes = rootGraph.getNodes();
//        for (SNode node : nodes) {
//            if (node instanceof SparkTraceGraph) {
//                foundGraph.add((SparkTraceGraph) node);
//            } else {
//                List<SparkTraceGraph> subFoundGraph = findNestedSparkTraceGraph(rootGraph);
//                foundGraph.addAll(subFoundGraph);
//            }
//        }
//        return foundGraph;
        return null;
    }


}
