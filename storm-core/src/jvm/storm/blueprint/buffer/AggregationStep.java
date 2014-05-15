package storm.blueprint.buffer;

import java.io.Serializable;
import java.util.List;

/**
 * User: ecsark
 * Date: 5/14/14
 * Time: 4:20 PM
 */
public class AggregationStep implements Serializable {

    int outputPosition;

    int triggerPosition;

    List<Integer> inputPositions;

    public List<Integer> getInputPositions() {
        return inputPositions;
    }

    public int getOutputPosition() {
        return outputPosition;
    }

    public int getTriggerPosition() {
        return triggerPosition;
    }

    public AggregationStep(List<Integer> inputPositions, int outputPosition, int triggerPosition) {
        this.inputPositions = inputPositions;
        this.outputPosition = outputPosition;
        this.triggerPosition = triggerPosition;
    }
}