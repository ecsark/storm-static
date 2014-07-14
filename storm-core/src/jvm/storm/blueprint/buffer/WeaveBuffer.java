package storm.blueprint.buffer;

import backtype.storm.tuple.Tuple;

import java.util.ArrayList;
import java.util.List;

/**
 * User: ecsark
 * Date: 4/21/14
 * Time: 11:19 AM
 */
public class WeaveBuffer extends TupleBuffer implements IEntrance {

    protected List<WindowResultCallback> callbacks;

    protected Tuple[] tuples;
    protected int current;
    protected int cold;
    protected int head;
    protected int triggerIndex;
    protected List<Integer> partsInBlocks; // accumulative
    protected int blocksInWindow;
    protected int firstFullPosition;
    protected List<Integer> triggers;


    public WeaveBuffer(String id, int pace, int length, List<Integer> pass, int allPartLength) {
        this.pace = pace;
        this.id = id;
        this.length = length;

        partsInBlocks = new ArrayList<Integer>(pass);

        if (allPartLength < length) {
            for (int i=0; i<(length-1)/allPartLength; ++i) {
                int base = (i+1) * pass.get(pass.size()-1);
                for (int j : pass)
                    partsInBlocks.add(j+base);
            }
        }

        size = partsInBlocks.get(partsInBlocks.size()-1);
        tuples = new Tuple[size];

        triggers = new ArrayList<Integer>();

        if (length % pace == 0) {
            blocksInWindow = length / pace;
            triggers.addAll(partsInBlocks);
            triggerIndex = blocksInWindow - 1;
        }
        else {
            blocksInWindow = length / pace * 2 + 1;
            for (int i=0; i<partsInBlocks.size(); i+=2)
                triggers.add(partsInBlocks.get(i)-1);
            triggerIndex = length / pace;
        }


        firstFullPosition = partsInBlocks.get((blocksInWindow - 1)) - 1;
        cold = 0;
        head = 0;
        current = firstFullPosition;

        callbacks = new ArrayList<WindowResultCallback>();
    }

    public void addCallback (WindowResultCallback callback) {
        callbacks.add(callback);
    }

    @Override
    public void put(Tuple tuple) {

        if (cold < firstFullPosition) { // initial phase
            tuples[cold] = tuple;
            cold += 1;
        } else {
            tuples[current] = tuple;
            if (current == triggers.get(triggerIndex)) {
                partialAggregate();
                triggerIndex = (triggerIndex+1) % triggers.size();
            }
            current = (current+1) % size;
            head = (head+1) % size;
        }
    }

    void partialAggregate() {

        List<List<Object>> objs = new ArrayList<List<Object>>();

        for (int i=head; i!=current; i=(i+1)%size) {
            Tuple t = tuples[i];
            List<Object> tupleSelected = t.select(selectFields);
            objs.add(tupleSelected);
        }
        {
            Tuple t = tuples[current];
            List<Object> tupleSelected = t.select(selectFields);
            objs.add(tupleSelected);
        }

        Tuple result = new FakeTuple(function.apply(objs));

        for (WindowResultCallback callback : callbacks) {
            callback.process(result);
        }

    }
}
