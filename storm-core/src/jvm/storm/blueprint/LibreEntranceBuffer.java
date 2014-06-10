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

    List<LibreBuffer> entrances;
    List<List<Integer>> positions;
    List<Integer> size;


    LibreEntranceBuffer(List<LibreBuffer> tupleBuffer) {
        this.entrances = tupleBuffer;
        size = new ArrayList<Integer>();
        positions = new ArrayList<List<Integer>>();

        for (LibreBuffer entrance : entrances) {
            List<Integer> pos = new ArrayList<Integer>();
            pos.add(0);
            positions.add(pos);
            size.add(entrance.getSize());
            entrance.allowColdStart(false);
        }
    }

    @Override
    public void put(Tuple tuple) {
        for (int i=0; i<entrances.size(); ++i) {
            List<Integer> pos = positions.get(i);
            entrances.get(i).put(tuple, pos);
            pos.set(0, (pos.get(0) + 1) % size.get(i));
        }
    }
}
