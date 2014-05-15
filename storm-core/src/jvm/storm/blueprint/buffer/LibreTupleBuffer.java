package storm.blueprint.buffer;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import storm.blueprint.function.Functional;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * User: ecsark
 * Date: 5/14/14
 * Time: 11:12 AM
 */
public class LibreTupleBuffer implements Serializable {

    String id;

    protected Tuple[][] tuples;

    int nextResult;
    int layers;
    int size;
    int[] nextComponent;
    Fields selectFields;
    int pace;
    int length;

    Map<Integer, List<AggregationStrategy>> aggStrategies;

    /*
        The order of AggregationStrategy should be considered seriously!!!
     */
    public void addAggregationStrategy (AggregationStrategy strategy) {

        int triggerPosition = strategy.step.getTriggerPosition();

        if (triggerPosition >= size) {
            throw new IllegalArgumentException("Trigger position should be less than the size of tuple buffer");
        }

        if (!aggStrategies.containsKey(triggerPosition))
            aggStrategies.put(triggerPosition, new ArrayList<AggregationStrategy>());

        aggStrategies.get(triggerPosition).add(strategy);
    }


    public void addAggregationStrategy (Functional function, int triggerPosition, List<Integer> inputPositions,
                                        int outputPosition, List<LibreWindowCallback> callbacks) {

        addAggregationStrategy(new AggregationStrategy(function, inputPositions, outputPosition,
                triggerPosition, callbacks));
    }


    public LibreTupleBuffer(String id, int size, int layers, int pace, int length) {
        this.id = id;
        this.size = size;
        this.layers = layers;
        this.pace = pace;
        this.length = length;
        tuples = new Tuple[layers][size];

        nextResult = 0;

        nextComponent = new int[layers];
        for (int i=0; i<nextComponent.length; ++i)
            nextComponent[i] = 0;

        aggStrategies = new HashMap<Integer, List<AggregationStrategy>>();

    }

    public String getId () {
        return id;
    }

    public int getSize () {
        return size;
    }

    public int getPace () {
        return pace;
    }

    public int getLength () { return length;}

    public void setSelectFields (Fields selectFields) {
        this.selectFields = selectFields;
    }


    public void put (Tuple tuple, List<Integer> destComponentIds) {
        /*
            destComponentIds should be a sorted list in descending order
         */
        int windIndex = nextResult;

        for (int destComponent : destComponentIds) {
            while (nextComponent[windIndex] > destComponent) {
                windIndex = (windIndex + 1) % layers;
            }

            assert(nextComponent[windIndex] == destComponent);//TODO: remove this line

            tuples[windIndex][destComponent] = tuple;
            nextComponent[windIndex]++;

            //sub-aggregation and callback goes here
            partialAggregate(destComponent, windIndex);

            if (nextComponent[windIndex] == size) {
                nextResult = (nextResult + 1) % layers;
                nextComponent[windIndex] = 0;
            }
        }
    }


    private void partialAggregate (int trigger, int windIndex) {
        if (aggStrategies.containsKey(trigger)) {

            List<AggregationStrategy> stratList = aggStrategies.get(trigger);

            for (AggregationStrategy strategy : stratList) {

                List<List<Object>> objs = new ArrayList<List<Object>>();
                for (int inputPosition : strategy.step.inputPositions) {
                    Tuple t = tuples[windIndex][inputPosition];
                    List<Object> tupleSelected = t.select(selectFields);
                    objs.add(tupleSelected);
                }

                // calculate the result and save it
                Tuple result = new FakeTuple(strategy.function.apply(objs));
                tuples[windIndex][strategy.step.outputPosition] = result;

                // observer notification
                for (LibreWindowCallback callback : strategy.callbacks) {
                    //dest.tupleBuffer.put(result, dest.componentIds);
                    callback.process(result);
                }
            }
        }
    }

}

