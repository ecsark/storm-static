package storm.blueprint;

import java.util.*;

/**
 * User: ecsark
 * Date: 5/10/14
 * Time: 10:31 AM
 */
public class WindowManager {

    SortedMap<Integer, PaceGroup> windows; //sorted by the pace in the descending order

    public WindowManager() {
        windows = new TreeMap<Integer, PaceGroup>(Collections.reverseOrder());
    }

    public void put(String id, int windowLength, int pace) {
        if (!windows.containsKey(pace))
            windows.put(pace, new PaceGroup());
        PaceGroup paceGroup = windows.get(pace);

        paceGroup.add(id, windowLength, pace);
    }

    public void consolidate () {

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

}

