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
public class FullSlidingWindowBufferPro extends TupleBuffer {

    private List<FullWindowCallback> callbacks;
    private List<Integer> callbackStep;
    private List<Integer> callbackCounter;

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


    public FullSlidingWindowBufferPro(int windowLength, int pace) {
        assert(windowLength>0);
        tuples = new Tuple[windowLength];
        this.windowLength = windowLength;
        this.capacity = windowLength;
        head = 0;
        size = 0;
        step = 0;
        this.pace = pace;
        callbacks = new ArrayList<FullWindowCallback>();
        callbackCounter = new ArrayList<Integer>();
        callbackStep = new ArrayList<Integer>();
    }

    public FullSlidingWindowBufferPro(int windowLength) {
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

    public void addCallback(FullWindowCallback callback, int pace) {
        callbacks.add(callback);
        if (pace < 1)
            throw new IllegalArgumentException("Pace should be an integer greater than 0");
        callbackStep.add(pace);
        callbackCounter.add(0);
    }

    public void addCallback(FullWindowCallback callback) {
        addCallback(callback, 1);
    }


    private void onWindowFull() {

        for (int i=0; i<callbacks.size(); ++i) {
            FullWindowCallback callback = callbacks.get(i);
            int counter = callbackCounter.get(i) + 1;
            if (counter == callbackStep.get(i)) {
                provokeCallback(callback);
            }
            callbackCounter.set(i, counter%callbackStep.get(i));
        }
    }


    private void provokeCallback(FullWindowCallback callback) {
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
