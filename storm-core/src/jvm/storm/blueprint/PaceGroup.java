package storm.blueprint;

import java.io.Serializable;
import java.util.*;

/**
 * User: ecsark
 * Date: 5/10/14
 * Time: 12:02 PM
 */
public class PaceGroup implements Serializable{

    List<PaceItem> items;

    List<UseLink> links;

    Map<Integer, List<ResultDeclaration>> registry;

    int basePace;

    public PaceGroup () {
        items = new ArrayList<PaceItem>();
        registry = new HashMap<Integer, List<ResultDeclaration>>();
        links = new ArrayList<UseLink>();
    }

    public void add(String id, int windowLength, int pace) {
        items.add(new PaceItem(id, windowLength, pace));
    }

    public void sort () { // sort in the ascending order of <pace, windowLength>
        Collections.sort(items, new Comparator<PaceItem>() {
            @Override
            public int compare(PaceItem o1, PaceItem o2) {
                if (o1.pace == o2.pace)
                    return o1.windowLength - o2.windowLength;
                return o1.pace - o2.pace;
            }
        });
    }

    public void merge(PaceGroup another) {
        items.addAll(another.items);
    }

    // make sure there are no duplicate declarations
    private void register (int length, int start, int freq, String componentId) {
        if (!registry.containsKey(start)) {
            registry.put(start, new ArrayList<ResultDeclaration>());
        }
        List<ResultDeclaration> reg = registry.get(start);

        for (ResultDeclaration declaration : reg) {
            if (declaration.length==length && declaration.freq==freq) {
                return;
            }
        }
        reg.add(new ResultDeclaration(length, start, freq, componentId));

    }

    private ResultDeclaration findBestMatchingResult (int length, int start, int freq) {

        ResultDeclaration match = null;

        /*
            Qualification of a match:
                1) result length no greater than the expected
                2) identical start point
                3) pace being a divisor of the one of expected
         */
        if (registry.containsKey(start)) {

            int maxLength = -1;

            for (ResultDeclaration res : registry.get(start)) {
                if (res.length > maxLength && freq % res.freq == 0 && res.length <= length) {
                    maxLength = res.length;
                    match = res;
                }
            }
        }

        return match;
    }

    private void sanityCheck () {
        for (PaceItem item : items) {
            if (item.pace%basePace != 0)
                throw new RuntimeException("Window of pace " + Integer.toString(item.pace) +
                "should not be placed in PaceGroup " + Integer.toString(basePace));
        }
    }


    private void setUpBase () {

        if (items.size() > 0) {
            sort();
            basePace = items.get(0).pace;
            sanityCheck();
        }

        registry.clear();

        Set<Integer> separator = new HashSet<Integer>();
        for (PaceItem item : items) {
            separator.add(item.windowLength % basePace);
        }

        separator.add(0);
        separator.add(basePace);

        List<Integer> sep = new ArrayList<Integer>();
        Iterator<Integer> iter = separator.iterator();
        while(iter.hasNext()) {
            sep.add(iter.next());
        }

        Collections.sort(sep);

        for (int i=0; i<sep.size()-1; ++i) {
            register(sep.get(i+1)-sep.get(i), sep.get(i),
                    basePace, "__base_"+Integer.toString(i)+"__");
        }

    }


    private void partialAggregate (PaceItem item, int startingIndex, int endingIndex) {

        int position = 0;
        boolean open = false;
        int lastPosition = 0;

        int a = item.windowLength % item.pace;

        for (int i=startingIndex; i<endingIndex; ++i) {
            position += links.get(i).length;
            if (position%item.pace==0 || position%item.pace==a) {
                if (open) {
                    register(position - lastPosition, lastPosition % basePace, item.pace, item.id);
                    open = false;
                }
                lastPosition = position;
            } else {
                open = true;
            }
        }
    }

    public void organize () {

        setUpBase();

        links.clear();
        for (PaceItem item : items) {

            int start = 0, length = item.windowLength, freq = item.pace;

            int startingIndex = links.size();

            while (start < length) {
                ResultDeclaration match = findBestMatchingResult(length-start, start%basePace, freq);
                if (match == null) {
                    //TODO: should there be always one found?
                    throw new RuntimeException();
                }
                links.add(new UseLink(match.componentId, item.id, Parts.RESULT, match.length, freq));
                start += match.length;
            }

            // register partial result
            partialAggregate(item, startingIndex, links.size());

            // register result
            register(item.windowLength, 0, item.pace, item.id);

        }
    }

}

class PaceItem implements Serializable {
    String id;
    int windowLength;
    int pace;

    PaceItem (String id, int windowLength, int pace) {
        this.id = id;
        this.windowLength = windowLength;
        this.pace = pace;
    }
}

enum Parts {
    A, B, RESULT, REUSED,
}

class UseLink implements Serializable {
    String from, to;
    Parts part;
    int freq;
    int length;

    UseLink (String from, String to, Parts part, int length, int freq) {
        this.from = from;
        this.to = to;
        this.part = part;
        this.length = length;
        this.freq = freq;
    }
}

class ResultDeclaration implements Serializable {
    int length;
    int start;
    int freq;
    String componentId;
    // TODO: windId + componentId ?

    ResultDeclaration(int length, int start, int freq, String componentId) {
        this.length = length;
        this.start = start;
        this.freq = freq;
        this.componentId = componentId;
    }
}