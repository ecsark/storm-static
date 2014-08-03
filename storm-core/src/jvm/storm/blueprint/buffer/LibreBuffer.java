package storm.blueprint.buffer;

import backtype.storm.tuple.Tuple;
import storm.blueprint.function.Functional;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * User: ecsark
 * Date: 5/14/14
 * Time: 11:12 AM
 */
public class LibreBuffer extends TupleBuffer {

    protected Tuple[][] tuples;
    protected int[] nextComponent;
    protected List<Integer> ancestorStates;
    protected int nextResult;

    int layers;
    boolean coldStart = true; //TODO: make private
    int coldNextResult = 0;

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
                                        int outputPosition, List<WindowResultCallback> callbacks) {

        addAggregationStrategy(new AggregationStrategy(function, inputPositions, outputPosition,
                triggerPosition, callbacks));
    }

    public int getLayers() {
        return layers;
    }


    public LibreBuffer(String id, int size, int layers, int pace, int length) {
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

    public List<Integer> getAncestorStates () {
        return ancestorStates;
    }

    public void setAncestorStates (List<Integer> ancestorStates) {
        this.ancestorStates = ancestorStates;
    }

    public void allowColdStart (boolean allow) {
        coldStart = allow;
    }

    public void put (Tuple tuple, List<Integer> destComponentIds) {
        /*
            destComponentIds should be a sorted list in descending order
         */

        int windIndex = nextResult;
        int coldIndex = coldNextResult;

        for (int destComponent : destComponentIds) {

            if (coldStart) {
                while (coldIndex < ancestorStates.size()
                        && ancestorStates.get(coldIndex) > destComponent) {
                    ++coldIndex;
                }
                if (coldIndex < ancestorStates.size()) {
                    if (ancestorStates.get(coldIndex) != destComponent)
                        throw new RuntimeException("invalid");
                    if (destComponent == (size-1)) { // this window is finished!
                        ++coldNextResult;
                        if (coldNextResult >= ancestorStates.size())
                            coldStart = false; // cold start done

                    }
                    ancestorStates.set(coldIndex, destComponent+1);

                    continue; // skip the below
                }
            }

            while (nextComponent[windIndex] > destComponent) {
                windIndex = (windIndex + 1) % layers;
            }

/*
            if (nextComponent[windIndex] != destComponent) {//TODO: remove this line
                throw new RuntimeException("invalid");
            }
*/
            tuples[windIndex][destComponent] = tuple;
            nextComponent[windIndex]++;


            //sub-aggregation and callback goes here
            if (aggStrategies.containsKey(destComponent))
                partialAggregate(destComponent, windIndex);


            if (nextComponent[windIndex] == size) {
                nextResult = (nextResult + 1) % layers;
                nextComponent[windIndex] = 0;

            }
            windIndex = (windIndex + 1) % layers;// new added
        }

    }


    private void partialAggregate (int trigger, int windIndex) {

        List<AggregationStrategy> stratList = aggStrategies.get(trigger);

        for (AggregationStrategy strategy : stratList) {

            Tuple result = tuples[windIndex][trigger];

            if (strategy.step.inputPositions.size() > 1) { // perform aggregation if there are multiple inputs
                List<List<Object>> objs = new ArrayList<List<Object>>();

                for (int inputPosition : strategy.step.inputPositions) {
                    Tuple t = tuples[windIndex][inputPosition];
                    List<Object> tupleSelected = t.select(selectFields);
                    objs.add(tupleSelected);
                }

                // calculate the result and save it
                result = new FakeTuple(strategy.function.apply(objs));
            }

            tuples[windIndex][strategy.step.outputPosition] = result;

            // observer notification
            for (WindowResultCallback callback : strategy.callbacks) {
                callback.process(result);
            }
        }

    }

}

