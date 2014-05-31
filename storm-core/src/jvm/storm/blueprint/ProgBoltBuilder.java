package storm.blueprint;

import java.util.*;

/**
 * User: ecsark
 * Date: 5/31/14
 * Time: 9:38 PM
 */
public class ProgBoltBuilder {

    Map<Integer, RemainderGroup> windows;

    Set<String> windowNames;
    Random rand = new Random();

    ProgBoltBuilder () {
        windows = new HashMap<Integer, RemainderGroup>();
        windowNames = new HashSet<String>();
    }

    public ProgBoltBuilder addWindow(String id, int windowLength, int pace) {
        //make sure there are no id duplicates!
        while (windowNames.contains(id)) {
            id += "_" + rand.nextInt(100);
        }
        windowNames.add(id);

        if (!windows.containsKey(pace))
            windows.put(pace, new RemainderGroup(pace));
        RemainderGroup remainderGroup = windows.get(pace);

        remainderGroup.add(id, windowLength, pace);
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

        for (RemainderGroup remainderGroup : windows.values()) {
            remainderGroup.organize();
        }
    }
}
