package core.graphPipeline.basic;

/**
 * A transparent vertex is a vertex which don't modify the data schema. This node should have inputfields adn outputfileds
 * whose symbol values can match to each other
 */
public interface ITransparentVertex {
    void matchOutputToInput();

    IOTableCell getRelativeInputFiled(IOTableCell outputCell) throws Exception;

    void matchInputToOutput();
}
