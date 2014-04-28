package storm.blueprint.buffer;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import java.util.ArrayList;
import java.util.List;

/**
 * User: ecsark
 * Date: 4/21/14
 * Time: 11:32 AM
 */
public class IncrementalSlidingWindowBuffer extends TupleBuffer {

    public void setCallback(IncrementalWindowCallback callback) {
        this.callback = callback;
    }

    private IncrementalWindowCallback callback;

    protected Tuple[] tuples;
    protected int capacity;
    protected int head;
    protected int size;
    protected int pace;
    protected int step;
    protected int windowLength;

    public int getPace() {
        return pace;
    }

    public int getWindowLength() {
        return windowLength;
    }

    public Tuple getTuple(int index) {
        if (index>=windowLength) {
            throw new IndexOutOfBoundsException("Window length "+Integer.toString(windowLength));
        } else if (index < 0-pace) {
            throw new IndexOutOfBoundsException("Pace size "+Integer.toString(pace));
        }else if (index>=size) {
            return null;
        }

        return tuples[(index+head) % capacity];
    }


    public IncrementalSlidingWindowBuffer(int windowLength, int pace) {
        assert(windowLength>0);
        this.windowLength = windowLength;
        capacity = windowLength + pace;
        tuples = new Tuple[capacity];
        head = 0;
        size = 0;
        step = 0;
        this.pace = pace;
    }

    public IncrementalSlidingWindowBuffer(int windowLength) {
        this(windowLength, 1);
    }

    @Override
    public void put(Tuple tuple) {
        if (size < windowLength) {
            tuples[size] = tuple;
            size += 1;

            if (size == windowLength)
                onFirstWindowFull();

        } else {
            tuples[(head+windowLength)%capacity] = tuple;
            head = (head+1) % capacity;

            if (size == windowLength) {
                if (step == 0) // window is full
                    onWindowFull();
                step = (step+1) % pace;
            }
        }


    }

    private void onWindowFull() {
        if (callback != null) {
            Fields selectFields = callback.getInputFields();
            List<List<Object>> oldValues = new ArrayList<List<Object>>();
            for (int i=(head-pace)%capacity; i!=head; i=(i+1)%capacity) {
                List<Object> tupleSelected = tuples[i].select(selectFields);
                oldValues.add(tupleSelected);
            }
            List<List<Object>> newValues = new ArrayList<List<Object>>();
            for (int i=(head+windowLength-pace)%capacity; i!=(head+windowLength)%capacity; i=(i+1)%capacity) {
                List<Object> tupleSelected = tuples[i].select(selectFields);
                newValues.add(tupleSelected);
            }

            assert(oldValues.size()==newValues.size());

            callback.process(newValues, oldValues);
        }
    }

    private void onFirstWindowFull() {
        if (callback != null) {
            Fields selectFields = callback.getInputFields();
            List<List<Object>> tuples = new ArrayList<List<Object>>();
            for (int i=0; i<getWindowLength(); ++i) {
                Tuple t = this.tuples[(i+head) % capacity];
                List<Object> tupleSelected = t.select(selectFields);
                tuples.add(tupleSelected);
            }
            callback.initialize(tuples);
        }
    }
}
