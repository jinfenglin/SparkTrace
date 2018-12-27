package core.pipelineOptimizer;

import core.GraphSymbol.Symbol;
import core.GraphSymbol.SymbolTable;

import java.util.Map;

/**
 *
 */
public class SDFGraph extends SGraph {
    private String artifactIdColName;

    public SDFGraph(String artifactIdColName) {
        super();
        this.artifactIdColName = artifactIdColName;
    }


    /**
     * Assign values to the SDF input thus it can read dataFrames as user wish. The
     *
     * @param artifactIdColName Specify the column name of ID column
     * @param symbolValueMap    A map between symbol name and symbol value. Symbol name refer to the
     *                          symbols of SDF IOTable.
     */
    public static void configSDF(String artifactIdColName, Map<String, String> symbolValueMap, SDFGraph SDF) {
        SDF.setArtifactIdColName(artifactIdColName);
        for (String symbolName : symbolValueMap.keySet()) {
            Symbol symbol = SDF.getInputTable().getSymbolByVarName(symbolName);
            SymbolTable.setInputSymbolValue(symbol, symbolValueMap.get(symbolName));
        }
    }


    public String getArtifactIdColName() {
        return artifactIdColName;
    }

    public void setArtifactIdColName(String artifactIdColName) {
        this.artifactIdColName = artifactIdColName;
    }

}
