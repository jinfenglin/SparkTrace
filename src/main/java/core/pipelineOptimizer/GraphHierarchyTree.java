package core.pipelineOptimizer;

import core.graphPipeline.basic.IOTableCell;
import core.graphPipeline.basic.SGraph;
import core.graphPipeline.basic.Vertex;

import java.util.ArrayList;
import java.util.List;

/**
 * A tree record the hierarchy of a SGraph. A SGraph will construct the GHT, and all the SGraph within that SGraph
 * will form the subtree
 */
public class GraphHierarchyTree {
    private SGraph nodeContent;
    private GraphHierarchyTree parentTree;
    private List<GraphHierarchyTree> subTrees;

    public GraphHierarchyTree(GraphHierarchyTree parentTree, SGraph nodeContent) {
        this.nodeContent = nodeContent;
        this.parentTree = parentTree;
        subTrees = new ArrayList<>();
        buildTree(nodeContent);
    }

    private void buildTree(SGraph graph) {
        List<Vertex> nodes = graph.getNodes();
        for (Vertex vertex : nodes) {
            if (vertex instanceof SGraph) {
                GraphHierarchyTree subTree = new GraphHierarchyTree(this, (SGraph) vertex);
                subTrees.add(subTree);
            }
        }
    }

    /**
     * Lowest Common Ancestor
     */
    public LCAResult LCA(GraphHierarchyTree node1, GraphHierarchyTree node2) {
        LCAResult result = new LCAResult();
        List<GraphHierarchyTree> node1Path = new ArrayList<>();
        List<GraphHierarchyTree> node2Path = new ArrayList<>();
        boolean node1PathFound = findPath(this, node1, node1Path);
        boolean node2PathFound = findPath(this, node2, node2Path);

        if (node1PathFound && node2PathFound) {
            int min_length = Math.min(node1Path.size(), node2Path.size());
            int lcaIndex = 1;
            for (; lcaIndex <= min_length; lcaIndex++) {
                if (!node1Path.get(node1Path.size() - lcaIndex).equals(node2Path.get(node2Path.size() - lcaIndex))) {
                    break;
                }
            }
            lcaIndex -= 1;

            GraphHierarchyTree lcaNode = node1Path.get(node1Path.size() - lcaIndex);
            List<GraphHierarchyTree> node1ToLCAPath = node1Path.subList(0, node1Path.size() - lcaIndex);
            List<GraphHierarchyTree> node2ToLCAPath = node2Path.subList(0, node2Path.size() - lcaIndex);

            result.LCANode = lcaNode; //The root node
            result.node1ToLCAPath = node1ToLCAPath; //From the direct parent (inclusive) to root node (exclusive)
            result.node2ToLCAPath = node2ToLCAPath;

        }
        return result;
    }

    /**
     * Lowest Common Ancestor for a group of nodes
     *
     * @param nodes
     * @return
     */
    public GraphHierarchyTree LCA(GraphHierarchyTree... nodes) {
        GraphHierarchyTree lcaNode = LCA(nodes[0], nodes[1]).LCANode;
        for (int i = 2; i < nodes.length; i++) {
            lcaNode = LCA(lcaNode, nodes[i]).LCANode;
        }
        return lcaNode;
    }

    public GraphHierarchyTree findNode(IOTableCell cell) {
        Vertex vertex = cell.getParentTable().getContext();
        return findNode(vertex);
    }

    public GraphHierarchyTree findNode(Vertex vertex) {
        if (nodeContent.containsNode(vertex))
            return this;
        for (GraphHierarchyTree subTree : subTrees) {
            GraphHierarchyTree searchNode = subTree.findNode(vertex);
            if (searchNode != null) {
                return searchNode;
            }
        }
        return null;
    }


    /**
     * Find a path from node source to node target
     *
     * @param source
     * @param target
     * @return
     */
    public boolean findPath(GraphHierarchyTree source, GraphHierarchyTree target, List<GraphHierarchyTree> path) {
        if (source == null) {
            return false;
        } else if (source == target) {
            path.add(target);
            return true;
        }
        for (GraphHierarchyTree subTree : source.getSubTrees()) {
            boolean isFound = findPath(subTree, target, path);
            if (isFound) {
                path.add(source);
                return true;
            }
        }
        return false;
    }

    /**
     * In the given tree node find the parent vertex that a child vertex reside in .
     *
     * @param treeNode
     * @param childVertex
     * @return
     */
    public Vertex findParentVertexInTreeNode(GraphHierarchyTree treeNode, Vertex childVertex) {
        GraphHierarchyTree curTreeNode = treeNode.findNode(childVertex);
        GraphHierarchyTree lastTreeNode = null; //When childVertexTreeNode equals treeNode, the lastFoundTreeNode record the subgraph childvertex resides in
        while (curTreeNode != treeNode) {
            lastTreeNode = curTreeNode;
            curTreeNode = curTreeNode.parentTree;
        }
        if (lastTreeNode == null) {
            //childVertex is a node in treeNode
            return childVertex;
        } else {
            //return the subGraph which contains
            return lastTreeNode.nodeContent;
        }
    }

    public SGraph getNodeContent() {
        return nodeContent;
    }

    public void setNodeContent(SGraph nodeContent) {
        this.nodeContent = nodeContent;
    }

    public GraphHierarchyTree getParentTree() {
        return parentTree;
    }

    public void setParentTree(GraphHierarchyTree parentTree) {
        this.parentTree = parentTree;
    }

    public List<GraphHierarchyTree> getSubTrees() {
        return subTrees;
    }

    public void setSubTrees(List<GraphHierarchyTree> subTrees) {
        this.subTrees = subTrees;
    }
}
