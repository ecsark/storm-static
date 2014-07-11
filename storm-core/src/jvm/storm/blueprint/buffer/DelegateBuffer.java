package storm.blueprint.buffer;

import backtype.storm.tuple.Tuple;

import java.util.ArrayList;
import java.util.List;

/**
 * User: ecsark
 * Date: 6/1/14
 * Time: 11:42 AM
 */
public class DelegateBuffer extends TupleBuffer {

    protected Tuple[] tuples;
    protected List<Integer> partSize;
    protected List<Integer> accumulatedSize;
    List<WindowResultCallback> callbacks;

    int currentPartIndex;
    int currentStep;


    public DelegateBuffer(String id, List<Integer> partSize, int length) {

        if (partSize.size() < 1)
            throw new IllegalArgumentException("there should be at least one element in partSize!");
        this.id = id;
        this.partSize = partSize;
        currentPartIndex = 0;
        currentStep = 0;
        accumulatedSize = new ArrayList<Integer>();
        this.length = length;
        size = 0;
        int maxSize = 0;
        for (int ps : partSize) {
            size += ps;
            accumulatedSize.add(size);
            if (maxSize < ps) maxSize = ps;
        }

        tuples = new Tuple[maxSize];
        callbacks = new ArrayList<WindowResultCallback>();
    }


    public void addCallback (WindowResultCallback callback) {
        callbacks.add(callback);
    }

    public void put (Tuple tuple) {

        tuples[currentStep] = tuple;

        ++currentStep;

        if (currentStep == partSize.get(currentPartIndex)) {

            partialAggregate(currentStep);

            currentPartIndex = (currentPartIndex+1) % partSize.size();
            currentStep = 0;
        }

    }

    private void partialAggregate (int range) {

        List<List<Object>> objs = new ArrayList<List<Object>>();
        for (int i=0; i<range; ++i) {
            List<Object> tupleSelected = tuples[i].select(selectFields);
            objs.add(tupleSelected);
        }
        Tuple result = new FakeTuple(function.apply(objs));

        for (WindowResultCallback callback : callbacks) {
            callback.process(result);
        }
    }

}
