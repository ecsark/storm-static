package storm.blueprint.buffer;

import backtype.storm.tuple.Tuple;
import storm.blueprint.LibreEntranceBuffer;

import java.util.ArrayList;
import java.util.List;

/**
 * User: ecsark
 * Date: 7/7/14
 * Time: 1:51 PM
 */
public class ColdBuffer extends FullBuffer implements IEntrance {

    int start;
    int counter = 0;
    int maxTimes = -1;
    LibreEntranceBuffer entrance;

    public void register (int totalTimes) {
        if (totalTimes > maxTimes)
            maxTimes = totalTimes;
    }

    public void setEntrance (LibreEntranceBuffer entrance) {
        this.entrance = entrance;
    }

    public ColdBuffer(String id, int size, int pace, int start) {
        super(id, size, pace, size);
        this.start = start;
    }

    @Override
    void partialAggregate() {

        List<List<Object>> objs = new ArrayList<List<Object>>();

        for (int i=start; i< getSize(); ++i) {
            Tuple t = tuples[(i+head) % size];
            List<Object> tupleSelected = t.select(selectFields);
            objs.add(tupleSelected);
        }

        Tuple result = new FakeTuple(function.apply(objs));

        for (WindowResultCallback callback : callbacks) {
            callback.process(result);
        }

        counter++;
        if (counter >= maxTimes)
            entrance.detach(this);

    }


}
