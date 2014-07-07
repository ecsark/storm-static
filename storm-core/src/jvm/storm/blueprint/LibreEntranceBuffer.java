package storm.blueprint;

import backtype.storm.tuple.Tuple;
import storm.blueprint.buffer.ColdBuffer;
import storm.blueprint.buffer.IEntrance;
import storm.blueprint.buffer.LibreBuffer;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * User: ecsark
 * Date: 5/19/14
 * Time: 11:15 AM
 */
public class LibreEntranceBuffer implements IEntrance {

    List<LibreBuffer> entrances;
    List<List<Integer>> positions;
    List<Integer> sizes;

    List<ColdBuffer> coldBuffers;
    List<Integer> detachList;


    LibreEntranceBuffer(List<LibreBuffer> tupleBuffer) {
        this.entrances = tupleBuffer;
        sizes = new ArrayList<Integer>();
        positions = new ArrayList<List<Integer>>();

        coldBuffers = new ArrayList<ColdBuffer>();
        detachList = new ArrayList<Integer>();

        for (LibreBuffer entrance : entrances) {
            List<Integer> pos = new ArrayList<Integer>();
            pos.add(0);
            positions.add(pos);
            sizes.add(entrance.getSize());
            entrance.allowColdStart(false);
        }
    }

    public void addColdBuffer (ColdBuffer coldBuffer) {
        coldBuffers.add(coldBuffer);
        coldBuffer.setEntrance(this);
    }

    // called during putToColdBuffers() by ColdBuffer
    public boolean detach (ColdBuffer coldBuffer) {
        int index = coldBuffers.indexOf(coldBuffer);
        if (index < 0)
            return false;
        else {
            detachList.add(index);
            return true;
        }
    }

    private void detach () {
        Iterator<ColdBuffer> iter = coldBuffers.iterator();
        iter.next();
        int i = 0;
        for (int index : detachList) {
            while (i < index) {
                iter.next();
                i++;
            }
            iter.remove();
        }
        detachList.clear();
    }

    private void putToColdBuffers (Tuple tuple) {
        for (ColdBuffer buf : coldBuffers) {
            buf.put(tuple);
        }
        if (detachList.size() > 0)
            detach();
    }

    int counter = 0;

    @Override
    public void put(Tuple tuple) {

        for (int i=0; i<entrances.size(); ++i) {
            List<Integer> pos = positions.get(i);
            entrances.get(i).put(tuple, pos);
            pos.set(0, (pos.get(0) + 1) % sizes.get(i));
        }

        if (coldBuffers.size() > 0)
            putToColdBuffers(tuple);
    }

}
