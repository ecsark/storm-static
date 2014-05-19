package storm.blueprint;

import backtype.storm.tuple.Tuple;
import storm.blueprint.buffer.LibreTupleBuffer;
import storm.blueprint.buffer.TupleBuffer;

import java.util.ArrayList;
import java.util.List;

/**
 * User: ecsark
 * Date: 5/19/14
 * Time: 11:15 AM
 */
public class LibreEntranceBuffer extends TupleBuffer {

    LibreTupleBuffer buf;
    List<Integer> position;
    int size;

    LibreEntranceBuffer(LibreTupleBuffer tupleBuffer) {
        this.buf = tupleBuffer;
        size = buf.getSize();
        position = new ArrayList<Integer>();
        position.add(0);
    }

    @Override
    public void put(Tuple tuple) {
        buf.put(tuple, position);
        position.set(0, (position.get(0)+1)%size);
    }
}
