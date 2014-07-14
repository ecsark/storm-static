package storm.blueprint;

import storm.blueprint.buffer.ColdBuffer;
import storm.blueprint.buffer.LibreBuffer;
import storm.blueprint.util.Divisor;
import storm.blueprint.util.ListMap;
import storm.blueprint.util.SetMap;

import java.util.*;

/**
 * User: ecsark
 * Date: 7/4/14
 * Time: 6:39 PM
 */
public class SuperBoltBuilder extends AutoBoltBuilder {

    transient ListMap<Integer, WindowItem> windows; // pace -> [query...]
    transient SetMap<Integer, Integer> groups; // pace -> [remainder...]
    transient List<Integer> entrances;

    SuperBoltBuilder() {
        super();
        windows = new ListMap<Integer, WindowItem>();
        groups = new SetMap<Integer, Integer>();
        entrances = new ArrayList<Integer>();
    }


    @Override
    public SuperBoltBuilder addWindow(String id, int windowLength, int pace) {

        String windowName = uniqueWindowName(id);

        WindowItem window = new WindowItem(windowName, windowLength, pace);
        windows.put(pace, window);

        groups.put(pace, windowLength % pace);

        return this;
    }


    protected Map<Integer, List<Integer>> makeTopology() {

        List<Integer> paces = new ArrayList<Integer>();
        paces.addAll(windows.map.keySet());
        Collections.sort(paces);

        int base = Divisor.getGreatestCommonDivisor(paces);
        ListMap<Integer, Integer> topology = new ListMap<Integer, Integer>(); // pace -> [receiver pace...]
        topology.touch(base);

        for (int pace : paces) {
            int maxDivisible = base;
            for (int d : topology.getMap().keySet()) {
                if (pace%d == 0 && d > maxDivisible)
                    maxDivisible = d;
            }

            List<Integer> divisors = Divisor.getDivisors(pace/maxDivisible);  // ordered

            for (int divisor : divisors) {
                int newDivisible = maxDivisible * divisor;
                topology.put(maxDivisible, newDivisible);
                maxDivisible = newDivisible;
                topology.touch(maxDivisible);
            }
        }

        // generate key list in descending order
        List<Integer> delegates = new ArrayList<Integer>(topology.keySet());
        Collections.sort(delegates, new Comparator<Integer>() {
            @Override
            public int compare(Integer o1, Integer o2) {
                return o2 - o1;
            }
        });

        // do it again: to find if there is a better predecessor
        for (int delegate : delegates) {
            int maxDivisible = base;
            for (int d : topology.keySet()) {
                if (delegate%d == 0 && d > maxDivisible && delegate > d)
                    maxDivisible = d;
            }

            for (Map.Entry<Integer,List<Integer>> e : topology.entrySet()) {
                if (e.getValue().contains(delegate)) {
                    if (e.getKey() < maxDivisible) {
                        e.getValue().remove((Integer) delegate);
                        topology.get(maxDivisible).add(delegate);
                    }
                    break;
                }
            }
        }

        // remove unnecessary receivers
        List<Integer> keysToRemove = new ArrayList<Integer>();

        List<Integer> receiversToAdd = new ArrayList<Integer>();

        for (Map.Entry<Integer, List<Integer>> e : topology.entrySet()) {

            receiversToAdd.clear();
            Iterator<Integer> recIterator = e.getValue().iterator();
            while (recIterator.hasNext()) {
                int receiver = recIterator.next();
                if (!paces.contains(receiver) ) { // not a query
                    if (topology.get(receiver).size() < 1) {
                        recIterator.remove();
                        keysToRemove.add(receiver);
                    } else if (topology.get(receiver).size() == 1 && receiver == e.getKey()*2) { // a 2-fold increment
                        recIterator.remove();
                        receiversToAdd.add(topology.get(receiver).get(0));
                        topology.get(receiver).clear();
                        keysToRemove.add(receiver);
                    }
                }
            }

            e.getValue().addAll(receiversToAdd);
        }

        // remove unnecessary keys
        for (int key : keysToRemove) {
            if (key > base)
                topology.getMap().remove(key);
        }

        return topology.getMap();
    }


    private void buildDelegate (int pace, String id) {

        List<Integer> r = new ArrayList<Integer>(groups.get(pace));
        Collections.sort(r);

        boolean isEntrance = id.startsWith("__entrance");

        int start = 0;
        for (int i=1; i<r.size(); ++i) {
            // construction
            while (start < r.get(i)) {
                ResultDeclaration declaration = Registry.getLongest(Registry.find(pace, start, r.get(i)-start, isEntrance));
                Registry.reuse(new UseLink(id, declaration, start, pace));
                start += declaration.length;
            }

            // declare each individual block
            Registry.declare(new ResultDeclaration(r.get(i)-r.get(i-1), r.get(i-1), pace, id));
            // declare cumulative result
            if (i > 1)
                Registry.declare(new ResultDeclaration(r.get(i), 0, pace, id));

        }
    }

    private void buildWindow (WindowItem w) {

        // construction
        int start = 0;
        List<Integer> parts = new ArrayList<Integer>();
        while (start < w.length) {
            ResultDeclaration declaration = Registry.getLongest(Registry.find(w.pace, start, w.length-start));
            Registry.reuse(new UseLink(w.id, declaration, start, w.pace));
            start += declaration.length;
            parts.add(start);
        }

        // declaration
        int lastStop = 0;
        for (int stop=0; stop<parts.size(); ++stop) {
            if (parts.get(stop)%w.pace == 0) {
                for (int j=stop-2; j>=lastStop; --j) {
                    Registry.declare(new ResultDeclaration(
                            parts.get(stop)-parts.get(j), // length
                            parts.get(j), // start
                            w.pace,w.id)); // pace, id
                }

                Registry.declare(new ResultDeclaration(parts.get(stop),0,w.pace,w.id));

                lastStop = stop;
            }
        }

        if (parts.get(parts.size()-1) < w.length) // check if the complete result has been declared above
            Registry.declare(new ResultDeclaration(w.length, 0, w.pace, w.id));

    }


    /*
     * TODO:
     *  ?? use parts from a window with larger pace when possible
     */

    @Override
    public AutoBolt build() {
        Map<Integer, List<Integer>> topology = makeTopology();

        // generate key list in descending order
        List<Integer> paces = new ArrayList<Integer>();
        paces.addAll(topology.keySet());
        Collections.sort(paces, new Comparator<Integer>() {
            @Override
            public int compare(Integer o1, Integer o2) {
                return o2 - o1;
            }
        });

        for (int pace : paces) {

            if (topology.get(pace).size() > 0) { // i.e., serves as an intermediate delegate
                if (!groups.containsKey(pace))
                    groups.touch(pace);
                Set<Integer> remainders = groups.get(pace);
                for (int receiver : topology.get(pace)) {
                    for (int rem : groups.get(receiver))
                        remainders.add(rem % pace);

                }
            }
        }

        // remainder % pace = 0 should be considered as remainder % pace = pace
        for (Map.Entry<Integer,Set<Integer>> e : groups.entrySet()) {
            Set<Integer> remainders = e.getValue();
            int pace = e.getKey();
            remainders.add(0);
            remainders.add(pace);
        }

        // build start!
        Collections.sort(paces);
        // these entrances take in raw data - declare only, no reuse
        int entrance = paces.get(0);
        if (entrance > 2) {
            entrances.add(entrance);
        } else { // if entrance is 1 or 2
            entrances.addAll(topology.get(entrance));
            if (windows.containsKey(entrance)) { // if it's the pace of a query
                entrances.add(entrance);
            } else {
                paces.remove(0);
                windows.getMap().remove(entrance);
                groups.getMap().remove(entrance);
            }
        }

        Registry.declare(new ResultDeclaration(1,0,1,"__unit__"));

        // build delegates
        for (int pace : paces) {
            if (entrances.contains(pace))
                buildDelegate(pace, "__entrance"+pace+"__");
            else
                buildDelegate(pace, "__delegate"+pace+"__");
        }

        // build window in the ascending order of <pace, windowLength>
        List<Integer> queryPaces = new ArrayList<Integer>(windows.keySet());
        Collections.sort(queryPaces);
        for (int pace: queryPaces) {
            List<WindowItem> winds = windows.get(pace);
            Collections.sort(winds, new Comparator<WindowItem>() {
                @Override
                public int compare(WindowItem o1, WindowItem o2) {
                    return o1.length - o2.length;
                }
            });
            for (WindowItem window : winds)
                buildWindow(window);
        }

        // build window buffer
        List<WindowItem> allWindows = windows.values();

        // make delegate WindowItem
        for (int pace : groups.keysSet()) {
            WindowItem delegateWindow = null;
            if (entrances.contains(pace))
                delegateWindow = new WindowItem("__entrance"+pace+"__", pace, pace);
            else
                delegateWindow = new WindowItem("__delegate"+pace+"__", pace, pace);
            delegateWindow.setEmitting(false);
            allWindows.add(delegateWindow);
        }

        // build buffer!
        LibreBufferBuilder bufferBuilder = new LibreBufferBuilder();
        Collection<LibreBuffer> newBuffers = bufferBuilder.build(Registry.getLinks(), allWindows, function,
                inputFields, bolt);
        buffers.clear();
        buffers.addAll(newBuffers);

        // set entrance buffer
        List<LibreBuffer> entranceBuffers = new ArrayList<LibreBuffer>();
        for (LibreBuffer buf : newBuffers) {
            if (buf.getId().startsWith("__entrance"))
                entranceBuffers.add(buf);
        }

        cells = 0;
        for (LibreBuffer buf : newBuffers)
            cells += buf.getSize();

        LibreEntranceBuffer entranceBuffer = new LibreEntranceBuffer(entranceBuffers);

        // build coldBuffer!
        List<ColdBuffer> coldBuffers = new ColdBufferBuilder().build(bufferBuilder.buffers,
                bufferBuilder.partitions, function, inputFields);

        for (ColdBuffer buf : coldBuffers) {
            entranceBuffer.addColdBuffer(buf);
        }

        bolt.setEntrance(entranceBuffer);


        return bolt;
    }
}
