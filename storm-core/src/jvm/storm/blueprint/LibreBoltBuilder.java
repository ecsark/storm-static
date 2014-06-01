package storm.blueprint;

import storm.blueprint.buffer.LibreBuffer;
import storm.blueprint.buffer.TupleBuffer;

import java.util.*;

/**
 * User: ecsark
 * Date: 5/10/14
 * Time: 10:31 AM
 */
public class LibreBoltBuilder extends AutoBoltBuilder {

    SortedMap<Integer, PaceGroup> windows; //sorted by the pace in the descending order

    List<LibreBuffer> entrances;


    public LibreBoltBuilder() {
        super();
        windows = new TreeMap<Integer, PaceGroup>(Collections.reverseOrder());
    }

    @Override
    public LibreBoltBuilder addWindow(String id, int windowLength, int pace) {

        //make sure there are no id duplicates!
        while (windowNames.contains(id)) {
            id += "_" + rand.nextInt(100);
        }
        windowNames.add(id);

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

    public AutoBolt build() {

        consolidate();

        LibreBufferBuilder bufferBuilder = new LibreBufferBuilder();
        buffers = new ArrayList<TupleBuffer>();
        entrances = new ArrayList<LibreBuffer>();

        for (PaceGroup paceGroup : windows.values()) {

            Collection<LibreBuffer> newBuffers = bufferBuilder.build(paceGroup, function, inputFields, bolt);
            buffers.addAll(newBuffers);

            // find the entrance
            // remember base window is placed at the last!
            String entranceName = paceGroup.windows.get(paceGroup.windows.size()-1).id;

            for (LibreBuffer buf : newBuffers) {
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

