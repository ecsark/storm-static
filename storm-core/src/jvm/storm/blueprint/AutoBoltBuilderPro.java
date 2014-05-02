package storm.blueprint;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import storm.blueprint.buffer.*;
import storm.blueprint.function.FunctionNotSupportedException;
import storm.blueprint.function.Incremental;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * User: ecsark
 * Date: 4/21/14
 * Time: 6:11 PM
 */
public class AutoBoltBuilderPro implements Serializable {

    public static String FULL_WINDOW = "full";
    public static String INCREMENTAL_WINDOW = "incr";
    public static String HIERARCHICAL_WINDOW = "hier";

    private transient AutoBoltPro _bolt;

    private static FunctionFactory ff = new FunctionFactory();

    public AutoBoltBuilderPro(String boltName) {
        _bolt = new AutoBoltPro();
        _bolt.boltName = boltName;
        _bolt.stack = new HashMap<String, BufferStack>();
    }

    public AutoBoltBuilderPro setEntrance (String bufferName) {
        if (!_bolt.stack.containsKey(bufferName))
            throw new RuntimeException(bufferName + " is not found in the buffer stack!");
        _bolt.entrance = _bolt.stack.get(bufferName).buffer;
        return this;
    }

    public AutoBoltBuilderPro setInputFields (String... fields) {
        _bolt.inputFields = new Fields(fields);
        return this;
    }

    public AutoBoltBuilderPro setOutputFields (String... fields) {
        _bolt.outputFields = new Fields(fields);
        return this;
    }

    public AutoBoltBuilderPro addTupleBuffer (String outputStreamId, int windowLength, int pace, String functionName,
                                              String windowType)
            throws FunctionNotSupportedException, BufferTypeNotSupportedException {
        return addTupleBuffer(outputStreamId, windowLength, pace, functionName, windowType, true);
    }

    public AutoBoltBuilderPro addTupleBuffer (String outputStreamId, int windowLength, int pace, String functionName,
                                              String windowType, boolean emitting)
            throws FunctionNotSupportedException, BufferTypeNotSupportedException {

        BufferStack stack = new BufferStack();
        stack.function = ff.getFunction(functionName);
        stack.outputStreamId = outputStreamId;
        stack.chainedBuffers = new ArrayList<BufferStack.BufferCounter>();

        if (pace < 1 || windowLength < 1)
            throw new IllegalArgumentException("Window length and pace should be greater than 0");

        if (windowType.toLowerCase().equals(FULL_WINDOW)) {
            stack.buffer = createFullSlidingWindowBuffer(windowLength, pace, stack, _bolt, emitting);

        } else if (windowType.toLowerCase().equals(INCREMENTAL_WINDOW)) {
            stack.buffer = createIncrementalSlidingWindowBuffer(windowLength, pace, stack, _bolt, emitting);

        } else {
            throw new BufferTypeNotSupportedException(windowType + " is not supported");
        }

        _bolt.stack.put(outputStreamId, stack);
        return this;
    }

    public AutoBoltBuilderPro pushToStack (String reporter, String listener, int pace) {
        if (pace < 1)
            throw new IllegalArgumentException("Listener's pace should be greater than 0");

        if (!_bolt.stack.containsKey(reporter) || !_bolt.stack.containsKey(listener))
            throw new IllegalArgumentException("Make sure the reporter and listener have been properly registered");

        _bolt.stack.get(reporter).addListenerBuffer(_bolt.stack.get(listener).buffer, pace);

        return this;
    }

    private FullSlidingWindowBuffer createFullSlidingWindowBuffer(int windowLength, int pace,
                                                                  final BufferStack stack,
                                                                  final AutoBoltPro bolt,
                                                                  final boolean emitting) {
        FullSlidingWindowBuffer buffer = new FullSlidingWindowBuffer(windowLength, pace);
        buffer.setCallback(new FullWindowCallback() {

            @Override
            public void process(List<List<Object>> tuples) {
                Values result = stack.function.apply(tuples);
                if (emitting)
                    bolt.collector.emit(stack.outputStreamId, new Values(result.get(0)));
                stack.propagate(result);
            }

            @Override
            public Fields getInputFields() {
                return bolt.inputFields;
            }
        });

        return buffer;
    }

    private IncrementalSlidingWindowBuffer createIncrementalSlidingWindowBuffer(int windowLength, int pace,
                                                                                final BufferStack stack,
                                                                                final AutoBoltPro bolt,
                                                                                final boolean emitting) {
        IncrementalSlidingWindowBuffer buffer = new IncrementalSlidingWindowBuffer(windowLength, pace);

        buffer.setCallback(new IncrementalWindowCallback() {


            @Override
            public void process(List<List<Object>> newTuples, List<List<Object>> oldTuples) {
                stack.state = ((Incremental) stack.function).update(newTuples, oldTuples, stack.state);
                if (emitting)
                    bolt.collector.emit(stack.outputStreamId, new Values(stack.state.get(0)));
                stack.propagate(stack.state);
            }

            @Override
            public void initialize(List<List<Object>> initialTuples) {
                if (!(stack.function instanceof Incremental))
                    throw new RuntimeException(stack.outputStreamId + "'s function should be Incremental");
                stack.state = stack.function.apply(initialTuples);
            }

            @Override
            public Fields getInputFields() {
                return bolt.inputFields;
            }
        });

        return buffer;
    }

    public AutoBoltPro build () {
        if (_bolt.stack.size()==0) {
            throw new RuntimeException("There should be at least one tuple buffer");
        } else if (_bolt.entrance==null) {
            throw new RuntimeException("The entrance is not set yet");
        } else if (_bolt.inputFields==null) {
            throw new RuntimeException(_bolt.boltName +"'s input field is not set yet");
        } else if (_bolt.outputFields==null) {
            throw new RuntimeException(_bolt.boltName +"'s output field is not set yet");
        }
        return _bolt;
    }
}
