package storm.blueprint.buffer;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import java.util.ArrayList;
import java.util.List;

/**
 * User: ecsark
 * Date: 4/21/14
 * Time: 11:19 AM
 */
public class FullSlidingWindowBuffer extends TupleBuffer {

    private FullWindowCallback callback;

    protected Tuple[] tuples;
    protected int windowLength;
    protected int capacity;
    protected int head;
    protected int size;
    protected int pace;
    protected int step;

    public int getPace() {
        return pace;
    }

    public int getWindowLength() {
        return windowLength;
    }

    public Tuple getTuple(int index) {
        if (index<0 || index>=windowLength) {
            throw new IndexOutOfBoundsException("Window length "+Integer.toString(capacity));
        } else if (index>=size) {
            return null;
        }

        return tuples[(index+head) % capacity];
    }


    public FullSlidingWindowBuffer(int windowLength, int pace) {
        assert(windowLength>0);
        tuples = new Tuple[windowLength];
        this.windowLength = windowLength;
        this.capacity = windowLength;
        head = 0;
        size = 0;
        step = 0;
        this.pace = pace;
    }

    public FullSlidingWindowBuffer(int windowLength) {
        this(windowLength, 1);
    }

    @Override
    public void put(Tuple tuple) {
        if (size < windowLength) {
            tuples[size] = tuple;
            size += 1;
        } else {
            tuples[head] = tuple;
            head = (head+1) % capacity;
        }

        if (size == windowLength) {
            if (step == 0) // window is full
                onWindowFull();
            step = (step+1) % pace;
        }
    }

    public void setCallback(FullWindowCallback callback) {
        this.callback = callback;
    }

    private void onWindowFull() {
        if (callback != null) {
            Fields selectFields = callback.getInputFields();
            List<List<Object>> objs = new ArrayList<List<Object>>();
            for (int i=0; i<getWindowLength(); ++i) {
                Tuple t = tuples[(i+head) % capacity];
                List<Object> tupleSelected = t.select(selectFields);
                objs.add(tupleSelected);
            }
            callback.process(objs);
        }
    }
}
