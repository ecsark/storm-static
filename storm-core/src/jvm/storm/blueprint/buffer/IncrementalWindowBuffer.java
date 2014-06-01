package storm.blueprint.buffer;

import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import storm.blueprint.function.Incremental;

import java.util.ArrayList;
import java.util.List;

/**
 * User: ecsark
 * Date: 4/21/14
 * Time: 11:32 AM
 */
public class IncrementalWindowBuffer extends TupleBuffer implements IEntrance {

    private List<WindowResultCallback> callbacks;

    protected Tuple[] tuples;
    protected int capacity;
    protected int head;
    protected int currentSize;
    protected int step;

    private Incremental func;
    private Values state;

    public IncrementalWindowBuffer(String id, int size, int pace, int length) {
        this.pace = pace;
        this.id = id;
        this.size = size;
        this.length = length;

        capacity = size + pace;
        tuples = new Tuple[capacity];

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

            if (currentSize == size)
                onFirstWindowFull();

        } else {
            tuples[(head+ size)%capacity] = tuple;
            head = (head+1) % capacity;

            if (currentSize == size) {
                if (step == 0) // window is full
                    partialAggregate();
                step = (step+1) % pace;
            }
        }
    }


    private void partialAggregate() {

        List<List<Object>> oldValues = new ArrayList<List<Object>>();
        for (int i=(head-pace+capacity)%capacity; i!=head; i=(i+1)%capacity) {
            List<Object> tupleSelected = tuples[i].select(selectFields);
            oldValues.add(tupleSelected);
        }

        List<List<Object>> newValues = new ArrayList<List<Object>>();
        for (int i=(head+ size -pace)%capacity; i!=(head+ size)%capacity; i=(i+1)%capacity) {
            List<Object> tupleSelected = tuples[i].select(selectFields);
            newValues.add(tupleSelected);
        }

        assert(oldValues.size()==newValues.size());

        state = func.update(newValues, oldValues, state);

        Tuple result = new FakeTuple(state);

        for (WindowResultCallback callback : callbacks) {
            callback.process(result);
        }
    }


    private void onFirstWindowFull() {

        if (!(function instanceof Incremental)) {
            throw new RuntimeException ("Function for IncrementalWindow has to be incremental");
        }
        func = (Incremental) function;

        List<List<Object>> tuples = new ArrayList<List<Object>>();
        for (int i=0; i<size; ++i) {
            Tuple t = this.tuples[(i+head) % capacity];
            List<Object> tupleSelected = t.select(selectFields);
            tuples.add(tupleSelected);
        }
        state = function.apply(tuples);

        Tuple result = new FakeTuple(state);

        for (WindowResultCallback callback : callbacks) {
            callback.process(result);
        }

    }
}
