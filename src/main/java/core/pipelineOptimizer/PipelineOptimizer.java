package core.pipelineOptimizer;


import core.graphPipeline.SLayer.*;
import core.graphPipeline.basic.IOTable;
import core.graphPipeline.basic.IOTableCell;
import core.graphPipeline.basic.ITransparentVertex;
import core.graphPipeline.basic.Vertex;
import core.graphPipeline.graphSymbol.Symbol;
import featurePipelineStages.SGraphIOStage;


import java.util.*;
import java.util.stream.Collectors;

import static core.graphPipeline.SLayer.SGraph.topologicalSort;

/**
 *
 */
public class PipelineOptimizer {

    /**
     * During optimization some new filed will be generated and inserted into the graph. To avoid the conflict between
     * this names and the user defined name we use the format of source/target filed name + uuid. It can increase the readability
     * and make the name unique.
     *
     * @return
     */
    public static String createUniqueNewFieldName(IOTableCell cell) {
        return cell.getFieldSymbol().getSymbolName() + "-" + UUID.randomUUID();
    }

    /**
     * If the output filed targetCell generate exactly same content as sourceCell, then user can use this function can
     * transfer the dependency of targetCell to sourceCell. penetration can reduce duplicated work and further simplify
     * the graph structure in redundancy removal procedures.
     *
     * @param sourceCell the IOTableCell which provide content
     * @param targetCell The IOTableCell whose content will duplicate the sourceCell
     */
    public static void penetrate(IOTableCell sourceCell, IOTableCell targetCell, GraphHierarchyTree treeRoot) throws Exception {
        GraphHierarchyTree sourceTree = treeRoot.findNode(sourceCell);
        GraphHierarchyTree targetTree = treeRoot.findNode(targetCell);
        LCAResult lcaResult = treeRoot.LCA(sourceTree, targetTree);
        List<GraphHierarchyTree> sourceToLCAPath = lcaResult.node1ToLCAPath;
        List<GraphHierarchyTree> targetToLCAPath = lcaResult.node2ToLCAPath;

        Vertex sourceTopParentNode = null;
        Vertex targetTopParentNode = null;
        String sourceOutputFieldName = sourceCell.getFieldSymbol().getSymbolName();

        String addedOutputFieldName = createUniqueNewFieldName(sourceCell);
        String addedInputFieldName = createUniqueNewFieldName(targetCell);

        if (sourceToLCAPath.size() > 0) {
            boolean isPenetratedVertex = true;
            //Add the sourceCell as an output to all the SGraph in the sourceToLCAPath
            Vertex subGraph = sourceCell.getParentTable().getContext();
            for (GraphHierarchyTree treeNode : sourceToLCAPath) {
                SGraph curGraph = treeNode.getNodeContent();
                Symbol newOutField = new Symbol(curGraph, addedOutputFieldName);//expand the output filed of parent graph
                curGraph.addOutputField(newOutField);
                if (isPenetratedVertex) {
                    curGraph.connect(subGraph, sourceOutputFieldName, curGraph.sinkNode, addedOutputFieldName);
                    isPenetratedVertex = false;
                } else {
                    curGraph.connect(subGraph, addedOutputFieldName, curGraph.sinkNode, addedOutputFieldName);
                }
                subGraph = curGraph;
            }
            sourceTopParentNode = sourceToLCAPath.get(sourceToLCAPath.size() - 1).getNodeContent();
        } else {
            //The source cell belong to a SNode in the LCA. No need to add the output to sinkNode
            sourceTopParentNode = sourceCell.getParentTable().getContext();
            addedOutputFieldName = sourceOutputFieldName; //No field added then use the origin field of the node

        }

        if (targetToLCAPath.size() > 0) {
            //Add the targetCell as an input to all the SGraph in the targetToLCAPath
            boolean isPenetratedVertex = true; //The first vertex we process is where penetrated filed reside.
            Vertex subGraph = null; // starting from the graph that contains penetrated vertex, there is not sub graph

            for (GraphHierarchyTree treeNode : targetToLCAPath) {
                SGraph curGraph = treeNode.getNodeContent();
                Symbol newInputField = new Symbol(curGraph, addedInputFieldName);
                curGraph.addInputField(newInputField);
                if (isPenetratedVertex) {
                    List<IOTableCell> linkedCells = new ArrayList<>(targetCell.getOutputTarget());
                    for (IOTableCell linkedCell : linkedCells) {
                        //Transfer the dependency of penetrated node to the sourceNode
                        Vertex fromVertex = targetCell.getParentTable().getContext();
                        String fromFieldName = targetCell.getFieldSymbol().getSymbolName();
                        Vertex toVertex = linkedCell.getParentTable().getContext();
                        String toFieldName = linkedCell.getFieldSymbol().getSymbolName();
                        curGraph.disconnect(fromVertex, fromFieldName, toVertex, toFieldName);
                        curGraph.connect(curGraph.sourceNode, addedInputFieldName, toVertex, toFieldName);
                        curGraph.sourceNode.getOutputField(addedInputFieldName).setRemovable(false);
                    }
                    isPenetratedVertex = false;
                } else {
                    curGraph.connect(curGraph.sourceNode, addedInputFieldName, subGraph, addedInputFieldName);
                }
                subGraph = curGraph;
            }
            targetTopParentNode = targetToLCAPath.get(targetToLCAPath.size() - 1).getNodeContent();
        } else {
            //The targetCell belong to a SNode in LCA. Need to transfer the dependency to the source parent directly.
            targetTopParentNode = targetCell.getParentTable().getContext();
            addedInputFieldName = targetCell.getFieldSymbol().getSymbolName();
        }

        SGraph lcaNode = lcaResult.LCANode.getNodeContent();
        if (targetTopParentNode instanceof SGraph) {
            lcaNode.connect(sourceTopParentNode, addedOutputFieldName, targetTopParentNode, addedInputFieldName);
        } else {
            List<IOTableCell> linkedCells = targetCell.getOutputTarget();
            for (IOTableCell linkedCell : new ArrayList<>(linkedCells)) {
                //Transfer the dependency of penetrated field to the sourceField
                Vertex linkedVertex = linkedCell.getParentTable().getContext();
                String linkedFieldName = linkedCell.getFieldSymbol().getSymbolName();
                //String sourceCellFiledName = sourceCell.getFieldSymbol().getSymbolName(); //
                String targetCellFieldName = targetCell.getFieldSymbol().getSymbolName();
                lcaNode.disconnect(targetTopParentNode, targetCellFieldName, linkedVertex, linkedFieldName);
                lcaNode.connect(sourceTopParentNode, addedOutputFieldName, linkedVertex, linkedFieldName);
            }
        }
    }

    /**
     * If the sourceNode is same as the targetNode, then this function can transfer the dependency of target node to source Node.
     * Vertex level Penetration can be used in automated optimization to reduce the duplicated part of graph.
     *
     * @param sourceVertex
     * @param targetVertex
     * @param treeRoot
     */
    public static void penetrate(Vertex sourceVertex, Vertex targetVertex, GraphHierarchyTree treeRoot) throws Exception {
        int sourceOutputLen = sourceVertex.getOutputTable().getCells().size();
        int targetOutputLen = targetVertex.getOutputTable().getCells().size();
        assert sourceOutputLen == targetOutputLen;
        for (int i = 0; i < sourceOutputLen; i++) {
            IOTableCell sourceCell = sourceVertex.getOutputTable().getCells().get(i);
            IOTableCell targetCell = targetVertex.getOutputTable().getCells().get(i);
            penetrate(sourceCell, targetCell, treeRoot);
        }
    }

    /**
     * Remove the duplicated node from the graph without recursion
     *
     * @param graph
     */
    public static void removeDuplicatedNodes(SGraph graph) throws Exception {
        //Put all nodes in the search list as init
        // while search list not empty
        // Partition the node in list into buckets then in each buckets
        //       partition the buckets by the inputs. Eliminate the input-bucket partition with penetration, add the impacted nodes into search list
        Map<SGraph, List<Vertex>> topoOrderMap = buildTopologicalOrderMap(graph);
        GraphHierarchyTree ght = new GraphHierarchyTree(null, graph);
        List<SNode> searchPool = getAllSNodesRecursively(graph);
        while (searchPool.size() > 0) {
            List<List<SNode>> snodeFamilies = groupByNode(searchPool);
            List<List<SNode>> duplicatedSNodeGroups = groupByInputs(snodeFamilies);
            // exclude the IOStage groups
            duplicatedSNodeGroups = duplicatedSNodeGroups.stream().filter(
                    group -> !(group.get(0).getSparkPipelineStage() instanceof SGraphIOStage)
            ).collect(Collectors.toList());
            searchPool = resolveDuplication(duplicatedSNodeGroups, topoOrderMap, ght);
        }
    }

    private static Map<SGraph, List<Vertex>> buildTopologicalOrderMap(SGraph graph) throws Exception {
        Map<SGraph, List<Vertex>> tpOrderMap = new HashMap<>();
        tpOrderMap.put(graph, topologicalSort(graph));
        for (Vertex vertex : graph.getNodes()) {
            if (vertex instanceof SGraph) {
                Map<SGraph, List<Vertex>> subMap = buildTopologicalOrderMap((SGraph) vertex);
                tpOrderMap.putAll(subMap);
            }
        }
        return tpOrderMap;
    }

    /**
     * Eliminate a list of duplicated graph nodes.
     *
     * @param identicalNodeGroups A list of nodes which are identified as exactly same.
     * @param topoOrderMap        A hashMap for topological sort
     * @param treeRoot            GraphHierarchyTree whose root is current graph
     * @return
     */
    public static List<SNode> resolveDuplication(List<List<SNode>> identicalNodeGroups, Map<SGraph, List<Vertex>> topoOrderMap, GraphHierarchyTree treeRoot) throws Exception {
        List<SNode> impactedNode = new ArrayList<>(); //Record nodes whose input has been changed
        for (List<SNode> nodeGroup : identicalNodeGroups) {
            if (nodeGroup.size() > 1) {
                SNode master = selectMaster(nodeGroup, treeRoot, topoOrderMap); //Select the node whose topological index is lower as master
                for (SNode node : nodeGroup) {
                    impactedNode.addAll(traceImpactedNodes(node));
                }
                for (SNode node : nodeGroup) {
                    if (node != master) {
                        penetrate(master, node, treeRoot);
                    }
                }
            }
        }
        return impactedNode;
    }

    /**
     * Collect the nodes which receive the output from the given node. If any of the nodes are IONode,
     * then keep searching the nodes which consume the output of that IONode
     */
    private static List<SNode> traceImpactedNodes(SNode node) {
        List<SNode> impNodes = new ArrayList<>();
        for (Vertex vertex : node.getOutputVertices()) {
            if (vertex instanceof SNode) {
                SNode impactNode = (SNode) vertex;
                if (impactNode.getSparkPipelineStage() instanceof SGraphIOStage) {
                    for (Vertex sinkRelatedVertex : impactNode.getContext().getOutputVertices()) {
                        if (sinkRelatedVertex instanceof SNode) {
                            if (((SNode) sinkRelatedVertex).getSparkPipelineStage() instanceof SGraphIOStage) {
                                impNodes.addAll(traceImpactedNodes((SNode) sinkRelatedVertex));
                            } else {
                                impNodes.add((SNode) sinkRelatedVertex);
                            }
                        } else {
                            impNodes.addAll(traceImpactedNodes(((SGraph) sinkRelatedVertex).sourceNode));
                        }
                    }
                } else {
                    impNodes.add(impactNode);
                }
            } else {
                impNodes.addAll(traceImpactedNodes(((SGraph) vertex).sourceNode));
            }
        }
        return impNodes;
    }

    private static SNode selectMaster(List<SNode> nodes, GraphHierarchyTree ght, Map<SGraph, List<Vertex>> topoOrderMap) {
        List<GraphHierarchyTree> treeNodes = new ArrayList<>();
        for (SNode node : nodes) {
            GraphHierarchyTree treeNode = ght.findNode(node);
            treeNodes.add(treeNode);
        }

        GraphHierarchyTree lcaTreeNode = ght.LCA(treeNodes.toArray(new GraphHierarchyTree[0]));
        List<Pair<Vertex, SNode>> parentVertices = new ArrayList<>();
        for (SNode node : nodes) {
            Vertex parentVertex = ght.findParentVertexInTreeNode(lcaTreeNode, node);
            parentVertices.add(new Pair<>(parentVertex, node));
        }
        //Sort the parentVertices base on their topological order index, from low to high
        List<Vertex> topoOrder = topoOrderMap.get(lcaTreeNode.getNodeContent());
        Collections.sort(parentVertices, new Comparator<Pair<Vertex, SNode>>() {
            @Override
            public int compare(Pair<Vertex, SNode> o1, Pair<Vertex, SNode> o2) {
                return topoOrder.indexOf(o1.getKey()) - topoOrder.indexOf(o2.getKey());
            }
        });
        return parentVertices.get(0).getValue();
    }

    public static List<List<SNode>> groupByNode(List<SNode> snodeList) {
        Map<String, List<SNode>> nodeGroups = new HashMap<>();
        for (SNode node : snodeList) {
            List<SNode> nodeGroup = nodeGroups.getOrDefault(node.nodeContentInfo(), new ArrayList<>());
            nodeGroup.add(node);
            nodeGroups.put(node.nodeContentInfo(), nodeGroup);
        }
        return new ArrayList<>(nodeGroups.values());
    }

    public static List<List<SNode>> groupByInputs(List<List<SNode>> snodeList) {
        List<List<SNode>> equalNodeGroups = new ArrayList<>(); //each list within this list contains identical nodes that should be merged
        for (List<SNode> nodeGroup : snodeList) {
            Map<InputSourceSet, List<SNode>> sameInputGroups = new HashMap<>(); //For each bucket that node have same stages, group the bucket by their input fields
            for (SNode node : nodeGroup) {
                InputSourceSet inputSource = new InputSourceSet(node); //represent the input source as string
                List<SNode> identicalNodeGroup = sameInputGroups.getOrDefault(inputSource, new ArrayList<>());
                identicalNodeGroup.add(node);
                sameInputGroups.put(inputSource, identicalNodeGroup);
            }
            equalNodeGroups.addAll(sameInputGroups.values());
        }
        return equalNodeGroups;
    }

    /**
     * Get all SNode from a graph. The SNode within the root graph will be collected recursively.
     *
     * @param graph
     * @return
     */
    public static List<SNode> getAllSNodesRecursively(SGraph graph) {
        List<SNode> nodes = new ArrayList<>();
        List<Vertex> vertices = graph.getNodes();
        for (Vertex vertex : vertices) {
            if (vertex instanceof SGraph) {
                List<SNode> subGraphNodes = getAllSNodesRecursively((SGraph) vertex);
                nodes.addAll(subGraphNodes);
            } else {
                nodes.add((SNode) vertex);
            }
        }
        return nodes;
    }


    private static int OutputFieldValidDegree(IOTableCell outputCell) throws Exception {
        int validDegree = 0; //count the numebr of valid connection
        for (IOTableCell targetCell : outputCell.getOutputTarget()) {
            Vertex targetVertex = targetCell.getParentTable().getContext();
            if (targetVertex instanceof ITransparentVertex) {
                ITransparentVertex tranVertex = (ITransparentVertex) targetVertex;
                IOTableCell transOut = tranVertex.getRelativeOutputField(targetCell); //get the next output
                validDegree += OutputFieldValidDegree(transOut);
            } else {
                validDegree += 1;
            }
        }
        return validDegree;
    }

    /**
     * Remove unused input and output field for the graph
     *
     * @param graph
     */
    public static void removeRedundantOutputFields(SGraph graph) throws Exception {
        //If the graph is a root graph then no removal otherwise do removal
        if (graph.getContext() != null) {
            for (IOTableCell outputCell : new ArrayList<>(graph.getOutputTable().getCells())) {
                //If the output filed not provide info to any outside node
                if (OutputFieldValidDegree(outputCell) == 0) {
                    //if(outputCell.getOutputTarget().size() == 0) {
                    graph.removeOutputField(outputCell.getFieldSymbol());
                }
            }
        }
        for (Vertex node : graph.getNodes()) {
            if (node instanceof SGraph) {
                SGraph graphNode = (SGraph) node;
                removeRedundantOutputFields(graphNode);
            }
        }
    }

    public static void removeRedundantInputFields(SGraph graph) {
        for (Vertex node : graph.getNodes()) {
            if (node instanceof SGraph) {
                SGraph graphNode = (SGraph) node;
                removeRedundantInputFields(graphNode);
            }
        }
        //If the graph is a root graph
        if (graph.getContext() == null) {
            return;
        }
        for (IOTableCell inputCell : graph.getInputTable().getCells()) {
            IOTableCell sourceOutputCell = graph.sourceNode.getOutputField(inputCell.getFieldSymbol().getSymbolName());
            //If the inputFiled not provide info to any inner node
            if (sourceOutputCell.getOutputTarget().size() == 0) {
                graph.removeInputField(inputCell.getFieldSymbol());
            }
        }
    }

    /**
     * Remove the graph which have no out-degree
     *
     * @param graph
     */
    public static int removeRedundantVertices(SGraph graph) throws Exception {
        int removedCnt = 0;
        List<Vertex> vertices = graph.getNodes();
        vertices.remove(graph.sourceNode);
        vertices.remove(graph.sinkNode);
        Queue<Vertex> deletionQueue = new LinkedList<>();
        for (Vertex node : vertices) {
            if (node.getOutputVertices().size() == 0) {
                deletionQueue.add(node);
            }
        }
        while (deletionQueue.size() > 0) {
            Vertex curNode = deletionQueue.poll();
            removedCnt += 1;
            graph.removeNode(curNode);
            Set<Vertex> inputVertices = curNode.getInputVertices();
            inputVertices.forEach(vertex -> {
                if (vertex.getOutputVertices().size() == 0) {
                    deletionQueue.add(vertex);
                }
            });
        }
        for (Vertex node : vertices) {
            if (node instanceof SGraph) {
                removedCnt += removeRedundantVertices((SGraph) node);
            }
        }
        return removedCnt;
    }

    public static void removeEmptyGraph(SGraph graph) throws Exception {
        Queue<SGraph> deletionQueue = new LinkedList<>();
        for (Vertex node : graph.getNodes()) {
            if (node instanceof SGraph) {
                SGraph graphNode = (SGraph) node;
                removeEmptyGraph(graphNode);
                if (graphNode.getNodes().size() == 2) {
                    deletionQueue.add(graphNode);
                }
            }
        }
        //by pass the empty graph first then delete the graph
        while (deletionQueue.size() > 0) {
            SGraph curGraph = deletionQueue.poll();
            IOTable inputTable = curGraph.getInputTable();
            for (IOTableCell inputCell : inputTable.getCells()) {
                for (IOTableCell outputCell : getDirectlyConnectedOutputCell(inputCell)) {
                    IOTableCell inputSourceCell = inputCell.getInputSource().get(0);
                    inputSourceCell.removeOutputTo(inputCell);
                    for (IOTableCell outputTarget : outputCell.getOutputTarget()) {
                        inputSourceCell.sendOutputTo(outputTarget);
                    }

                }
            }
            graph.removeNode(curGraph);
        }
    }

    /**
     * Find the directly linked sink outputfields for a inputcell
     *
     * @param inputCell
     */
    private static List<IOTableCell> getDirectlyConnectedOutputCell(IOTableCell inputCell) {
        List<IOTableCell> directlyConnectedGraphOutCells = new ArrayList<>();
        SGraph context = (SGraph) inputCell.getParentTable().getContext();
        IOTableCell sourceNodeOutCell = context.sourceNode.getOutputField(inputCell.getFieldSymbol().getSymbolName());
        List<IOTableCell> sourceNodeConnectTargets = sourceNodeOutCell.getOutputTarget();

        for (IOTableCell connectedInputCell : sourceNodeConnectTargets) {
            IOTableCell sinkNodeInputField = context.sinkNode.getInputField(connectedInputCell.getFieldSymbol().getSymbolName());
            if (sinkNodeInputField != null) { //Check iF the connctedInputcell belong to sinkNode
                IOTableCell sinkNodeInputCell = context.sinkNode.getInputField(sinkNodeInputField.getFieldSymbol().getSymbolName());
                IOTableCell graphOutputField = context.getOutputField(sinkNodeInputCell.getFieldSymbol().getSymbolName());
                directlyConnectedGraphOutCells.add(graphOutputField);
            }
        }
        return directlyConnectedGraphOutCells;
    }
}
