package storm.blueprint;

import backtype.storm.tuple.Fields;
import storm.blueprint.buffer.LibreTupleBuffer;
import storm.blueprint.function.Functional;

import java.io.Serializable;
import java.util.*;

/**
 * User: ecsark
 * Date: 5/10/14
 * Time: 10:31 AM
 */
public class LibreBoltBuilder implements Serializable {

    SortedMap<Integer, PaceGroup> windows; //sorted by the pace in the descending order

    List<LibreTupleBuffer> entrances;

    Collection<LibreTupleBuffer> buffers;

    Functional function;

    LibreBolt bolt;

    Fields inputFields;

    public LibreBoltBuilder setInputFields(Fields inputFields) {
        this.inputFields = inputFields;
        return this;
    }

    public LibreBoltBuilder setOutputFields(Fields outputFields) {
        bolt.setOutputFields(outputFields);
        return this;
    }

    public LibreBoltBuilder setFunction(Functional function) {
        this.function = function;
        return this;
    }

    public LibreBoltBuilder(String name) {
        windows = new TreeMap<Integer, PaceGroup>(Collections.reverseOrder());
        bolt = new LibreBolt();
        bolt.setBoltName(name);
    }

    public LibreBoltBuilder addWindow(String id, int windowLength, int pace) {
        if (!windows.containsKey(pace))
            windows.put(pace, new PaceGroup());
        PaceGroup paceGroup = windows.get(pace);

        paceGroup.add(id, windowLength, pace);
        return this;
    }

    protected void consolidate () {
        List<Integer> markToDelete = new ArrayList<Integer>();

        for (int pace : windows.keySet()) {
            for (int p : windows.keySet()) {
                if (pace%p==0 && pace>p) {
                    windows.get(p).merge(windows.get(pace));
                    markToDelete.add(pace);
                    break;
                }
            }
        }

        for (int pace : markToDelete) {
            windows.remove(pace);
        }

        for (PaceGroup paceGroup : windows.values()) {
            paceGroup.organize();
        }
    }

    public LibreBolt build() {

        consolidate();

        LibreBufferBuilder bufferBuilder = new LibreBufferBuilder();
        buffers = new ArrayList<LibreTupleBuffer>();
        entrances = new ArrayList<LibreTupleBuffer>();

        for (PaceGroup paceGroup : windows.values()) {

            Collection<LibreTupleBuffer> newBuffers = bufferBuilder.build(paceGroup, function, inputFields, bolt);
            buffers.addAll(newBuffers);

            // find the entrance
            String entranceName = paceGroup.windows.get(0).id;
            for (LibreTupleBuffer buf : newBuffers) {
                if (buf.getId().equals(entranceName)) {
                    entrances.add(buf);
                    break;
                }
            }
        }

        bolt.buffers = buffers;

        //TODO: correct this!
        entrances.get(0).allowColdStart(false);
        bolt.setEntrance(new LibreEntranceBuffer(entrances.get(0)));

        return bolt;
    }

}

