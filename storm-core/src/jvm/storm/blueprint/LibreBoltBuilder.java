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

    transient SortedMap<Integer, LibreGroup> windows; //sorted by the pace in the descending order

    transient List<LibreBuffer> entrances;


    public LibreBoltBuilder() {
        super();
        windows = new TreeMap<Integer, LibreGroup>(Collections.reverseOrder());
    }

    @Override
    public LibreBoltBuilder addWindow(String id, int windowLength, int pace) {

        //make sure there are no id duplicates!
        while (windowNames.contains(id)) {
            id += "_" + rand.nextInt(100);
        }
        windowNames.add(id);

        if (!windows.containsKey(pace))
            windows.put(pace, new LibreGroup());
        LibreGroup libreGroup = windows.get(pace);

        libreGroup.add(id, windowLength, pace);
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

        for (LibreGroup libreGroup : windows.values()) {
            libreGroup.organize();
        }
    }

    public AutoBolt build() {

        consolidate();

        LibreBufferBuilder bufferBuilder = new LibreBufferBuilder();
        buffers.clear();
        entrances = new ArrayList<LibreBuffer>();

        for (LibreGroup libreGroup : windows.values()) {

            Collection<LibreBuffer> newBuffers = bufferBuilder.build(libreGroup, function, inputFields, bolt);

            buffers.addAll(newBuffers);
            Collections.sort(buffers, new Comparator<TupleBuffer>() {
                @Override
                public int compare(TupleBuffer o1, TupleBuffer o2) {
                    if (o2.getPace()==o1.getPace())
                        return o2.getLength()-o1.getLength();
                    return o2.getPace()-o1.getPace();
                }
            });

            // find the entrance
            // remember base window is placed at the last!
            String entranceName = libreGroup.windows.get(libreGroup.windows.size()-1).id;

            for (LibreBuffer buf : newBuffers) {
                if (buf.getId().equals(entranceName)) {
                    entrances.add(buf);
                    break;
                }
            }
        }

        bolt.setEntrance(new LibreEntranceBuffer(entrances));

        return bolt;
    }

}

