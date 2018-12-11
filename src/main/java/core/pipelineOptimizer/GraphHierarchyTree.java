package core.pipelineOptimizer;

import java.util.ArrayList;
import java.util.Collections;
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
            int lcaIndex = 1;
            for (; lcaIndex < node1Path.size() && lcaIndex < node2Path.size(); lcaIndex++) {
                if (!node1Path.get(node1Path.size() - lcaIndex).equals(node2Path.get(node2Path.size() - lcaIndex))) {
                    break;
                }
            }
            GraphHierarchyTree lcaNode = node1Path.get(node1Path.size() - lcaIndex);
            List<GraphHierarchyTree> node1ToLCAPath = node1Path.subList(0, node1Path.size() - lcaIndex);
            List<GraphHierarchyTree> node2ToLCAPath = node2Path.subList(0, node2Path.size() - lcaIndex);

            result.LCANode = lcaNode;
            result.node1ToLCAPath = node1ToLCAPath;
            result.node2ToLCAPath = node2ToLCAPath;
        }
        return result;
    }

    public GraphHierarchyTree findNode(IOTableCell cell) {
        Vertex vertex = cell.getParentTable().getContext();
        if (nodeContent.containsNode(vertex))
            return this;
        for (GraphHierarchyTree subTree : subTrees) {
            GraphHierarchyTree searchNode = subTree.findNode(cell);
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
