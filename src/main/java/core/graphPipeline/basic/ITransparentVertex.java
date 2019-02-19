package core.graphPipeline.basic;

/**
 * A transparent vertex is a vertex which don't modify the data schema. This node should have inputfields adn outputfileds
 * whose symbol values can match to each other
 */
public interface ITransparentVertex {
    /**
     * The input and output are matched by their index. Given a outputCell find a inputcell with same index in IOTable.
     *
     * @param outputCell
     * @return
     * @throws Exception
     */
    IOTableCell getRelativeInputFiled(IOTableCell outputCell) throws Exception;

    IOTableCell getRelativeOutputField(IOTableCell inputField) throws Exception;

    void matchOutputToInput() throws Exception;

    void matchInputToOutput() throws Exception;
}
