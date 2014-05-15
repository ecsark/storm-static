package storm.blueprint.buffer;

import storm.blueprint.function.Functional;

import java.io.Serializable;
import java.util.List;

/**
 * User: ecsark
 * Date: 5/14/14
 * Time: 4:20 PM
 */
public class AggregationStrategy implements Serializable {

    AggregationStep step;
    Functional function;
    List<LibreWindowCallback> callbacks;

    public AggregationStrategy(Functional function, List<Integer> inputPositions, int outputPosition,
                        int triggerPosition, List<LibreWindowCallback> callbacks) {
        this.function = function;
        this.step = new AggregationStep(inputPositions, outputPosition, triggerPosition);
        this.callbacks = callbacks;
    }

    public AggregationStrategy(Functional function, AggregationStep step, List<LibreWindowCallback> callbacks) {
        this.function = function;
        this.step = step;
        this.callbacks = callbacks;
    }
}
