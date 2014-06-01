package storm.blueprint;

import backtype.storm.tuple.Tuple;
import storm.blueprint.buffer.IEntrance;
import storm.blueprint.buffer.LibreBuffer;

import java.util.ArrayList;
import java.util.List;

/**
 * User: ecsark
 * Date: 5/19/14
 * Time: 11:15 AM
 */
public class LibreEntranceBuffer implements IEntrance {

    LibreBuffer buf;
    List<Integer> position;
    int size;


    LibreEntranceBuffer(LibreBuffer tupleBuffer) {
        this.buf = tupleBuffer;
        size = buf.getSize();
        position = new ArrayList<Integer>();
        position.add(0);
        buf.allowColdStart(false);
    }

    @Override
    public void put(Tuple tuple) {

        buf.put(tuple, position);
        position.set(0, (position.get(0)+1)%size);
    }
}
