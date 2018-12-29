import core.graphPipeline.basic.*;
import core.pipelineOptimizer.*;
import examples.TestBase;
import featurePipeline.SGraphColumnRemovalStage;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.IDF;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;


/**
 *
 */
public class SGraphTest extends TestBase {
    private static String masterUrl = "local";

    public SGraphTest() {
        super(masterUrl);
    }

    private SGraph createGraph() throws Exception {
        Tokenizer tk = new Tokenizer();
        HashingTF htf1 = new HashingTF();
        HashingTF htf2 = new HashingTF();
        IDF idf = new IDF();

        SGraph g1 = new SGraph();
        g1.setId("graph");
        g1.addInputField("sentence");
        g1.addOutputField("output_idf");

        SNode n1 = new SNode(tk, "tokenizer");
        n1.addInputField("text");
        n1.addOutputField("tokens");

        SNode n2 = new SNode(htf1, "hashTF");
        n2.addInputField("token");
        n2.addOutputField("htf");

        SNode n3 = new SNode(idf, "IDF");
        n3.addInputField("true_htf");
        n3.addOutputField("idf");

        SNode n4 = new SNode(htf2, "dummy_htf");
        n4.addInputField("tokens");
        n4.addOutputField("n4_htf");

        g1.addNode(n1);
        g1.addNode(n2);
        g1.addNode(n3);
        g1.addNode(n4);

        g1.connect(g1.sourceNode, "sentence", n1, "text");
        g1.connect(n1, "tokens", n2, "token");
        g1.connect(n2, "htf", n3, "true_htf");
        g1.connect(n1, "tokens", n4, "tokens");
        g1.connect(n3, "idf", g1.sinkNode, "output_idf");

        return g1;
    }

    /**
     * Test the SGraph can create pipeline through SEdges. The edges contains no connections between symbols.
     * Thus the created pipeline should use default names as column names
     *
     * @throws Exception
     */
    @Test
    public void SGraphToPipelineTest() throws Exception {
        SGraph g1 = createGraph();
        Pipeline pipeline = g1.toPipeline();
        Assert.assertEquals(pipeline.getStages().length, 8);
    }

    @Test
    public void SGraphSourceNodeAndTokenizerNodeRelationshipTest() throws Exception {
        SGraph g1 = createGraph();
        SNode tokenizerNode = null;
        for (Vertex vertex : g1.getNodes()) {
            SNode node = (SNode) vertex;
            if (node.getSparkPipelineStage() instanceof Tokenizer) {
                tokenizerNode = node;
            }
        }
        IOTable inputTable = tokenizerNode.getInputTable();
        for (IOTableCell inputCell : inputTable.getCells()) {
            List<IOTableCell> parentCells = inputCell.getInputSource();
            Assert.assertEquals(1, parentCells.size());
            IOTableCell parentCell = parentCells.get(0);
            Vertex parentNode = parentCell.getFieldSymbol().getScope();
            Assert.assertEquals(parentNode, g1.sourceNode);
        }
    }

    /**
     * Create pipeline with graph and run the pipeline to generate idf for a corpus.
     */
    @Test
    public void idfCreationTest() throws Exception {
        Dataset<Row> dataset = getSentenceDataset();
        Pipeline pipeline = createGraph().toPipeline();
        printPipeline(pipeline);
        PipelineModel model = pipeline.fit(dataset);
        Dataset<Row> processedData = model.transform(dataset);
        processedData.show();
    }

    @Test
    public void penetrationTest() throws Exception {
        SGraph globalGraph = new SGraph();
        globalGraph.setId("top_graph");
        globalGraph.addInputField("sentence");

        globalGraph.addOutputField("output_idf");
        globalGraph.addOutputField("tokens");

        Tokenizer tk = new Tokenizer();
        SNode n1 = new SNode(tk, "global_tokenizer");
        n1.addInputField("text");
        n1.addOutputField("tokens");

        SGraph subGraph = createGraph();

        globalGraph.addNode(n1);
        globalGraph.addNode(subGraph);

        globalGraph.connect(globalGraph.sourceNode, "sentence", subGraph, "sentence");
        globalGraph.connect(globalGraph.sourceNode, "sentence", n1, "text");
        globalGraph.connect(n1, "tokens", globalGraph.sinkNode, "tokens");
        globalGraph.connect(subGraph, "output_idf", globalGraph.sinkNode, "output_idf");

        GraphHierarchyTree ght = new GraphHierarchyTree(null, globalGraph);
        PipelineOptimizer.penetrate(n1.getOutputField("tokens"), subGraph.getNode("tokenizer").getOutputField("tokens"), ght);

        Dataset<Row> dataset = getSentenceDataset();
        PipelineModel model = globalGraph.toPipeline().fit(dataset);
        Dataset<Row> processedData = model.transform(dataset);
        processedData.show();
    }

    @Test
    public void columnRemovalStageTest() {
        Dataset<Row> dataset = getSentenceDataset();
        SGraphColumnRemovalStage removalStage = new SGraphColumnRemovalStage();
        removalStage.setInputCols(new String[]{"sentence", "label"});
        dataset = removalStage.transform(dataset);
        Assert.assertEquals(0, dataset.columns().length);
    }
}
