package core.pipelineOptimizer;


import core.graphPipeline.basic.IOTableCell;
import core.graphPipeline.basic.SGraph;
import core.graphPipeline.basic.SNode;
import core.graphPipeline.basic.Vertex;
import core.graphPipeline.graphSymbol.Symbol;
import featurePipeline.NullRemoveWrapper.HasInnerStage;
import javafx.util.Pair;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.param.StringArrayParam;
import org.apache.spark.ml.param.shared.HasInputCol;
import org.apache.spark.ml.param.shared.HasInputCols;
import org.apache.spark.ml.param.shared.HasOutputCol;
import org.apache.spark.ml.param.Param;
import org.apache.spark.ml.param.shared.HasOutputCols;

import java.util.*;

/**
 *
 */
public class PipelineOptimizer {
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
        if (sourceToLCAPath.size() > 0) {
            //Add the sourceCell as an output to all the SGraph in the sourceToLCAPath
            Vertex subGraph = sourceCell.getParentTable().getContext();
            // TODO: Use unique name here to avoid name conflict
            // Since the outputField use name from sourceCell, it is possible that newOutField symbol conflict existing
            // name. A unique symbol name solve the problem.Its value won't change since this symbol receive value from
            // source cell and will not use its default symbol name as value.
            String addedOutputFieldName = sourceCell.getFieldSymbol().getSymbolName();
            for (GraphHierarchyTree treeNode : sourceToLCAPath) {
                SGraph curGraph = treeNode.getNodeContent();
                Symbol newOutField = new Symbol(curGraph, addedOutputFieldName);//expand the output filed of parent graph
                curGraph.addOutputField(newOutField);
                curGraph.connect(subGraph, addedOutputFieldName, curGraph.sinkNode, addedOutputFieldName);
                subGraph = curGraph;
            }
            sourceTopParentNode = sourceToLCAPath.get(sourceToLCAPath.size() - 1).getNodeContent();
        } else {
            //The source cell belong to a SNode in the LCA. No need to add the output to sinkNode
            sourceTopParentNode = sourceCell.getParentTable().getContext();
        }

        if (targetToLCAPath.size() > 0) {
            //Add the targetCell as an input to all the SGraph in the targetToLCAPath
            boolean isPenetratedVertex = true; //The first vertex we process is where penetrated filed reside.
            Vertex subGraph = targetCell.getParentTable().getContext();
            String addedInputFieldName = targetCell.getFieldSymbol().getSymbolName();

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
            }
            targetTopParentNode = targetToLCAPath.get(targetToLCAPath.size() - 1).getNodeContent();
        } else {
            //The targetCell belong to a SNode in LCA. Need to transfer the dependency to the source parent directly.
            targetTopParentNode = targetCell.getParentTable().getContext();
        }

        SGraph lcaNode = lcaResult.LCANode.getNodeContent();
        if (targetTopParentNode instanceof SGraph) {
            lcaNode.connect(sourceTopParentNode, sourceCell.getFieldSymbol().getSymbolName(), targetTopParentNode, targetCell.getFieldSymbol().getSymbolName());
        } else {
            List<IOTableCell> linkedCells = targetCell.getOutputTarget();
            for (IOTableCell linkedCell : linkedCells) {
                //Transfer the dependency of penetrated field to the sourceField
                Vertex linkedVertex = linkedCell.getParentTable().getContext();
                String linkedFieldName = linkedCell.getFieldSymbol().getSymbolName();
                String sourceCellFiledName = sourceCell.getFieldSymbol().getSymbolName();
                String targetCellFieldName = targetCell.getFieldSymbol().getSymbolName();
                lcaNode.disconnect(targetTopParentNode, targetCellFieldName, linkedVertex, linkedFieldName);
                lcaNode.connect(sourceTopParentNode, sourceCellFiledName, linkedVertex, linkedFieldName);
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
    public static void penetrate(Vertex sourceVertex, Vertex targetVertex, GraphHierarchyTree treeRoot) {
        return;
    }

    /**
     * Remove the duplicated node from the graph without recursion
     *
     * @param graph
     */
    public static void removeDuplicatedNodes(SGraph graph) {
        //Put all nodes in the search list as init
        // while search list not empty
        // Partition the node in list into buckets then in each buckets
        //       partition the buckets by the inputs. Eliminate the input-bucket partition with penetration, add the impacted nodes into search list
        Map<SGraph, List<Vertex>> topoOrderMap = buildTopologicalOrderMap();
        GraphHierarchyTree ght = new GraphHierarchyTree(null, graph);
        List<SNode> searchPool = getAllSNodesRecursively(graph);
        while (searchPool.size() > 0) {
            List<List<SNode>> snodeFamilies = groupByNode(searchPool);
            List<List<SNode>> duplicatedSNodeGroups = groupByInputs(snodeFamilies);
            searchPool = resolveDuplication(duplicatedSNodeGroups, topoOrderMap, ght);
        }
    }

    private static void

    /**
     * Eliminate a list of duplicated graph nodes.
     *
     * @param identicalNodeGroups A list of nodes which are identified as exactly same.
     * @param topoOrderMap        A hashMap for topological sort
     * @param treeRoot            GraphHierarchyTree whose root is current graph
     * @return
     */
    public static List<SNode> resolveDuplication(List<List<SNode>> identicalNodeGroups, Map<SGraph, List<Vertex>> topoOrderMap, GraphHierarchyTree treeRoot) {
        List<SNode> impactedNode = new ArrayList<>(); //Record nodes whose input has been changed
        for (List<SNode> nodeGroup : identicalNodeGroups) {
            if (nodeGroup.size() > 1) {
                SNode master = selectMaster(nodeGroup, treeRoot, topoOrderMap); //Select the node whose topological index is lower as master
                for (SNode node : nodeGroup) {
                    if (node != master) {
                        penetrate(master, node, treeRoot);
                    }
                }
            }
        }
        return impactedNode;
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
                return topoOrder.indexOf(o2.getKey()) - topoOrder.indexOf(o1.getKey());
            }
        });
        return parentVertices.get(0).getValue();
    }

    public static List<List<SNode>> groupByNode(List<SNode> snodeList) {
        Map<SNode, List<SNode>> nodeGroups = new HashMap<>();
        for (SNode node : snodeList) {
            List<SNode> nodeGroup = nodeGroups.getOrDefault(node, new ArrayList<>());
            nodeGroup.add(node);
            nodeGroups.put(node, nodeGroup);
        }
        return new ArrayList<>(nodeGroups.values());
    }

    public static List<List<SNode>> groupByInputs(List<List<SNode>> snodeList) {
        List<List<SNode>> equalNodeGroups = new ArrayList<>(); //each list within this list contains identical nodes that should be merged
        for (List<SNode> nodeGroup : snodeList) {
            Map<SNode, List<SNode>> sameInputGroups = new HashMap<>(); //For each bucket that node have same stages, group the bucket by their input fields
            for (SNode node : nodeGroup) {
                InputSourceSet inputSource = new InputSourceSet(node); //represent the input source as string
                List<SNode> identicalNodeGroup = sameInputGroups.getOrDefault(inputSource, new ArrayList<>());
                identicalNodeGroup.add(node);
                sameInputGroups.put(node, identicalNodeGroup);
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

    public static List<String> getNonIOParamsValue(PipelineStage stage) {
        List<Param> params = new ArrayList<>(Arrays.asList(stage.params()));
        List<String> paramValues = new ArrayList<>();
        if (stage instanceof HasInputCols) {
            params.remove(((HasInputCols) stage).inputCols());
        } else if (stage instanceof HasInputCol) {
            params.remove(((HasInputCol) stage).inputCol());
        }
        if (stage instanceof HasOutputCol) {
            params.remove(((HasOutputCol) stage).outputCol());
        } else if (stage instanceof HasOutputCols) {
            params.remove(((HasOutputCols) stage).outputCols());
        }
        for (Param param : params) {
            paramValues.add(stage.get(param).toString());
        }
        return paramValues;
    }

    public static boolean sameStageType(PipelineStage stage1, PipelineStage stage2) {
        if (stage1.getClass() != stage2.getClass()) {
            return false;
        }
        if (stage1 instanceof HasInnerStage) {
            PipelineStage innerStage1 = ((HasInnerStage) stage1).getInnerStage();
            PipelineStage innerStage2 = ((HasInnerStage) stage1).getInnerStage();
            return sameStageType(innerStage1, innerStage2);
        } else {
            return true;
        }
    }

    public static boolean sameConfig(PipelineStage stage1, PipelineStage stage2) {
        List<String> nonIOParams1 = getNonIOParamsValue(stage1);
        List<String> nonIOParams2 = getNonIOParamsValue(stage2);
        if (!nonIOParams1.equals(nonIOParams2))
            return false;
        if (stage1 instanceof HasInnerStage && stage2 instanceof HasInnerStage) {
            PipelineStage innerStage1 = ((HasInnerStage) stage1).getInnerStage();
            PipelineStage innerStage2 = ((HasInnerStage) stage1).getInnerStage();
            return sameConfig(innerStage1, innerStage2);
        } else if (!(stage1 instanceof HasInnerStage) && !(stage2 instanceof HasInnerStage)) {
            return true;
        } else {
            //If stage1 and stage2 inconsistent
            return false;
        }

    }

    /**
     * Remove the graph which have no out-degree
     *
     * @param graph
     */
    public static void removeRedundantVertices(SGraph graph) {
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
                removeDuplicatedNodes((SGraph) node);
            }
        }
    }

}
