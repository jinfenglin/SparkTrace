import core.GraphSymbol.Symbol;
import core.pipelineOptimizer.*;
import examples.TestBase;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.IDF;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Assert;
import org.junit.Test;

import javax.xml.crypto.Data;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class SGraphTest extends TestBase {
    private static String masterUrl = "local";

    public SGraphTest() {
        super(masterUrl);
    }

    private SGraph createGraph() {
        Tokenizer tk = new Tokenizer();
        HashingTF htf = new HashingTF();
        IDF idf = new IDF();

        SNode n1 = new SNode(tk, "tokenizer");
        n1.addInputField(new Symbol(n1, "text"));
        n1.addOutputField(new Symbol(n1, "tokens"));

        SNode n2 = new SNode(htf, "hashTF");
        n2.addInputField(new Symbol(n2, "token"));
        n2.addOutputField(new Symbol(n2, "htf"));

        SNode n3 = new SNode(idf, "IDF");
        n3.addInputField(new Symbol(n2, "true_htf"));
        n3.addOutputField(new Symbol(n2, "idf"));

        SNode n4 = new SNode(htf, "dummyTokenizer");
        n4.addInputField(new Symbol(n4, "tokens"));
        n4.addOutputField(new Symbol(n4, "n4_htf"));


        SGraph g1 = new SGraph();
        g1.addInputField(new Symbol(g1, "sentence"));
        g1.addOutputField(new Symbol(g1, "output_idf"));

        g1.addNode(n1);
        g1.addNode(n2);
        g1.addNode(n3);
        g1.addNode(n4);

        //Connect sourceNode and n1
        Map<Symbol, Symbol> er1Map = new HashMap<>();
        er1Map.put(g1.sourceNode.getOutputTable().getSymbolByVarName("sentence"), n1.getInputTable().getSymbolByVarName("text"));
        SEdge er1 = new SEdge(g1.sourceNode, n1, er1Map);

        //Connect n1 and n2
        Map<Symbol, Symbol> e12Map = new HashMap<>();
        e12Map.put(n1.getOutputTable().getSymbolByVarName("tokens"), n2.getInputTable().getSymbolByVarName("token"));
        SEdge e12 = new SEdge(n1, n2, e12Map);

        //Connect n2,n3
        Map<Symbol, Symbol> e23Map = new HashMap<>();
        e23Map.put(n2.getOutputTable().getSymbolByVarName("htf"), n3.getInputTable().getSymbolByVarName("true_htf"));
        SEdge e23 = new SEdge(n2, n3, e23Map);

        //connect n1,n4
        Map<Symbol, Symbol> e14Map = new HashMap<>();
        e14Map.put(n1.getOutputTable().getSymbolByVarName("tokens"), n4.getInputTable().getSymbolByVarName("tokens"));
        SEdge e14 = new SEdge(n1, n4, e14Map);

        //Connect n3 and sink node
        Map<Symbol, Symbol> e3eMap = new HashMap<>();
        e3eMap.put(n3.getOutputTable().getSymbolByVarName("idf"), g1.sinkNode.getInputTable().getSymbolByVarName("output_idf"));
        SEdge e3e = new SEdge(n4, n3, e3eMap);

        g1.addEdge(e12);
        g1.addEdge(e23);
        g1.addEdge(e14);
        g1.addEdge(e3e);
        g1.addEdge(er1);
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
        Assert.assertEquals(pipeline.getStages().length, 5);
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
        PipelineModel model = pipeline.fit(dataset);
        Dataset<Row> processedData = model.transform(dataset);
        processedData.show();
    }
}
