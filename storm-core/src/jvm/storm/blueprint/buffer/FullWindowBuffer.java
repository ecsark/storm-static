package storm.blueprint.buffer;

import backtype.storm.tuple.Tuple;

import java.util.ArrayList;
import java.util.List;

/**
 * User: ecsark
 * Date: 4/21/14
 * Time: 11:19 AM
 */
public class FullWindowBuffer extends TupleBuffer implements IEntrance {

    private List<WindowResultCallback> callbacks;

    protected Tuple[] tuples;
    protected int head;
    protected int currentSize;
    protected int step;


    public FullWindowBuffer(String id, int size, int pace, int length) {
        this.pace = pace;
        this.id = id;
        this.size = size;

        tuples = new Tuple[size];
        this.length = length;

        head = 0;
        currentSize = 0;
        step = 0;

        callbacks = new ArrayList<WindowResultCallback>();
    }

    public void addCallback (WindowResultCallback callback) {
        callbacks.add(callback);
    }

    @Override
    public void put(Tuple tuple) {
        if (currentSize < size) {
            tuples[currentSize] = tuple;
            currentSize += 1;
        } else {
            tuples[head] = tuple;
            head = (head+1) % size;
        }

        if (currentSize == size) {
            if (step == 0) // window is full
                partialAggregate();
            step = (step+1) % pace;
        }
    }

    private void partialAggregate() {

        List<List<Object>> objs = new ArrayList<List<Object>>();

        for (int i=0; i< getSize(); ++i) {
            Tuple t = tuples[(i+head) % size];
            List<Object> tupleSelected = t.select(selectFields);
            objs.add(tupleSelected);
        }

        Tuple result = new FakeTuple(function.apply(objs));

        for (WindowResultCallback callback : callbacks) {
            callback.process(result);
        }

    }
}
