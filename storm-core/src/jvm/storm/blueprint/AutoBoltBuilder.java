package storm.blueprint;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import storm.blueprint.buffer.*;
import storm.blueprint.function.FunctionNotSupportedException;
import storm.blueprint.function.Incremental;

import java.util.List;

/**
 * User: ecsark
 * Date: 4/21/14
 * Time: 6:11 PM
 */
public class AutoBoltBuilder {

    public static String FULL_WINDOW = "full";
    public static String INCREMENTAL_WINDOW = "incr";
    public static String HIERARCHICAL_WINDOW = "hier";

    AutoBolt autoBolt;

    private static FunctionFactory ff = new FunctionFactory();

    public AutoBoltBuilder(String name) {
        autoBolt = new AutoBolt();
        autoBolt.name = name;
    }

    public AutoBoltBuilder setFunction(String name) throws FunctionNotSupportedException {
        autoBolt.function = ff.getFunction(name);
        return this;
    }

    public AutoBoltBuilder setOutputFields(String... fields) {
        autoBolt.outputFields = new Fields(fields);
        return this;
    }

    public AutoBoltBuilder setInputSelectFields(String... fields) {
        autoBolt.inputFields = new Fields(fields);
        return this;
    }

    private FullSlidingWindowBuffer createFullSlidingWindowBuffer(int windowLength, int pace) {
        FullSlidingWindowBuffer buffer = new FullSlidingWindowBuffer(windowLength, pace);
        buffer.setCallback(new FullWindowCallback() {

            @Override
            public void process(List<List<Object>> tuples) {
                Values result = autoBolt.function.apply(tuples);
                autoBolt._collector.emit(new Values(result.get(0)));
            }

            @Override
            public Fields getInputFields() {
                return autoBolt.inputFields;
            }
        });

        return buffer;
    }

    private IncrementalSlidingWindowBuffer createIncrementalSlidingWindowBuffer(int windowLength, int pace) {
        IncrementalSlidingWindowBuffer buffer = new IncrementalSlidingWindowBuffer(windowLength, pace);
        buffer.setCallback(
                new IncrementalWindowCallback() {

                    @Override
                    public void process(List<List<Object>> newTuples, List<List<Object>> oldTuples) {
                        autoBolt.state = ((Incremental) autoBolt.function).update(
                                newTuples, oldTuples, autoBolt.state);
                        autoBolt._collector.emit(new Values(autoBolt.state.get(0)));
                    }

                    @Override
                    public void initialize(List<List<Object>> initialTuples) {
                        if (!(autoBolt.function instanceof Incremental))
                            throw new RuntimeException(autoBolt.name + "'s function should be Incremental");
                        autoBolt.state = autoBolt.function.apply(initialTuples);
                    }

                    @Override
                    public Fields getInputFields() {
                        return autoBolt.inputFields;
                    }
                }
        );

        return buffer;
    }

    private FullSlidingWindowBuffer createHierarchicalSlidingWindowBuffer (String name, int windowLength, int pace)
            throws BufferTypeNotSupportedException {
        FullSlidingWindowBuffer buffer = new FullSlidingWindowBuffer(pace, pace);

        //TODO: windowLength % pace != 0
        if (name.toLowerCase().equals(FULL_WINDOW))
            autoBolt.cacheBuffer = createFullSlidingWindowBuffer(windowLength/pace, 1);
        else if (name.toLowerCase().equals(INCREMENTAL_WINDOW))
            autoBolt.cacheBuffer = createIncrementalSlidingWindowBuffer(windowLength/pace, 1);
        else
            throw new BufferTypeNotSupportedException(name +
                    " is not supported for Hierarchical Sliding Window Buffer!");

        buffer.setCallback(
            new FullWindowCallback() {
                @Override
                public void process(List<List<Object>> tuples) {
                    Values result = autoBolt.function.apply(tuples);
                    Tuple fakeOutput = new FakeTuple(result);
                    autoBolt.cacheBuffer.put(fakeOutput);
                }

                @Override
                public Fields getInputFields() {
                    return autoBolt.inputFields;
                }
            }
        );

        return buffer;
    }

    public AutoBoltBuilder setTupleBuffer(String name, int windowLength, int pace) throws BufferTypeNotSupportedException {
        if (pace < 1 || windowLength < 1)
            throw new IllegalArgumentException("Window length and pace should be greater than 0");

        if (name.toLowerCase().equals(FULL_WINDOW)) {
            autoBolt.buffer = createFullSlidingWindowBuffer(windowLength, pace);

        } else if (name.toLowerCase().equals(INCREMENTAL_WINDOW)) {
            autoBolt.buffer = createIncrementalSlidingWindowBuffer(windowLength, pace);

        } else if (name.toLowerCase().startsWith(HIERARCHICAL_WINDOW)) {
            String upperName = name.substring(HIERARCHICAL_WINDOW.length()+1);
            autoBolt.buffer = createHierarchicalSlidingWindowBuffer(upperName, windowLength, pace);

        } else {
            throw new BufferTypeNotSupportedException(name + " is not supported");

        }
        return this;
    }

    public AutoBoltBuilder setTupleBuffer(String name, int windowLength) throws BufferTypeNotSupportedException {
        return setTupleBuffer(name, windowLength, 1);
    }

    public AutoBolt build() {
        if (autoBolt.function==null) {
            throw new RuntimeException(autoBolt.name+"'s function is not set yet");
        } else if (autoBolt.buffer==null) {
            throw new RuntimeException(autoBolt.name+"'s tuple buffer is not set yet");
        } else if (autoBolt.inputFields==null) {
            throw new RuntimeException(autoBolt.name+"'s input field is not set yet");
        } else if (autoBolt.outputFields==null) {
            throw new RuntimeException(autoBolt.name+"'s output field is not set yet");
        }
        return autoBolt;
    }
}
