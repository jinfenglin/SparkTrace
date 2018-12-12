package core.pipelineOptimizer;

import core.GraphSymbol.Symbol;

import java.util.ArrayList;
import java.util.List;

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
}
