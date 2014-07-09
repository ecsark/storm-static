package storm.blueprint;

import java.io.Serializable;
import java.util.*;

/**
 * User: ecsark
 * Date: 5/10/14
 * Time: 12:02 PM
 */
public class LibreGroup implements Serializable{

    List<WindowItem> windows;

    List<UseLink> links;

    transient Map<Integer, List<ResultDeclaration>> registry;

    int basePace;

    public LibreGroup() {
        windows = new ArrayList<WindowItem>();
        registry = new HashMap<Integer, List<ResultDeclaration>>();
        links = new ArrayList<UseLink>();
    }

    public void add(String id, int windowLength, int pace) {
        windows.add(new WindowItem(id, windowLength, pace));
    }

    public void sort () { // sort in the ascending order of <pace, length>
        Collections.sort(windows, new Comparator<WindowItem>() {
            @Override
            public int compare(WindowItem o1, WindowItem o2) {
                if (o1.pace == o2.pace)
                    return o1.length - o2.length;
                return o1.pace - o2.pace;
            }
        });
    }

    public void merge(LibreGroup another) {
        windows.addAll(another.windows);
    }

    // make sure there are no duplicate declarations
    private boolean register (int length, int start, int pace, String windowId) {
        if (!registry.containsKey(start%basePace)) {
            registry.put(start, new ArrayList<ResultDeclaration>());
        }
        List<ResultDeclaration> reg = registry.get(start%basePace);

        for (ResultDeclaration declaration : reg) {
            //if (declaration.length==length && declaration.pace==pace && declaration.start==start%pace) {
            if (declaration.length==length && declaration.pace==pace && declaration.start%pace==start%pace) {
                return false;
            }
        }
        //reg.add(new ResultDeclaration(length, start%pace, pace, windowId));
        reg.add(new ResultDeclaration(length, start, pace, windowId));
        return true;

    }

    private ResultDeclaration findBestMatchingResult (int length, int start, int pace) {

        ResultDeclaration match = null;

        /*
            Qualification of a match:
                1) result length no greater than the expected
                2) identical start point
                3) pace being a divisor of the one of expected
         */

        if (registry.containsKey(start%basePace)) {

            int maxLength = -1;

            for (ResultDeclaration res : registry.get(start%basePace)) {
                /*
                 *   TODO:
                 *         Actually a part can be reused
                 *         if start%res.pace==res.start%res.pace,
                 *         but the cold start problem has to be solved.
                 */
                if (res.length > maxLength && pace%res.pace == 0 && res.length <= length
                        && start%res.pace == res.start) {
                    maxLength = res.length;
                    match = res;
                }
            }
        }

        return match;
    }

    private void sanityCheck () {
        for (WindowItem item : windows) {
            if (item.pace%basePace != 0)
                throw new RuntimeException("Window of pace " + Integer.toString(item.pace) +
                "should not be placed in PaceGroup " + Integer.toString(basePace));
        }
    }


    private void setUpBase () {

        if (windows.size() > 0) {
            sort();
            basePace = windows.get(0).pace;
            sanityCheck();
        }

        registry.clear();

        Set<Integer> separator = new HashSet<Integer>();
        for (WindowItem item : windows) {
            separator.add(item.length % basePace);
        }

        separator.add(0);
        separator.add(basePace);

        List<Integer> sep = new ArrayList<Integer>();
        Iterator<Integer> iter = separator.iterator();
        while(iter.hasNext()) {
            sep.add(iter.next());
        }

        Collections.sort(sep);

        String baseName = "_base"+basePace;

        for (int i=0; i<sep.size()-1; ++i) {
            // register each block
            register(sep.get(i+1)-sep.get(i), sep.get(i),
                    basePace, baseName);
            // register cumulative block
            if (i > 1) {
                register(sep.get(i + 1), 0, basePace, baseName);
            }
        }

        // register a complete pace
        register(basePace, 0, basePace, baseName);

        // add baseWindow to windows
        WindowItem baseWindow = new WindowItem(baseName, basePace, basePace);
        baseWindow.setEmitting(false);
        windows.add(baseWindow); // NOTE HERE: base window is placed at the very rear!

        // construct base window using underlying unit window
        ResultDeclaration dummy = new ResultDeclaration(1,0,1,"_unit");
        for (int i=0; i<basePace; ++i) {
            links.add(new UseLink(baseName, dummy, i, basePace));
        }
    }

    private void construct(WindowItem item) {

        int start = 0, length = item.length, pace = item.pace;

        while (start < length) {
            ResultDeclaration match = findBestMatchingResult(length-start, start, pace);
            if (match == null) {
                //TODO: should there always be one found?
                throw new RuntimeException();
            }
            links.add(new UseLink(item.id, match, start, pace));
            start += match.length;
        }
    }


    private void partialAggregate (WindowItem item, int startingIndex, int endingIndex) {

        /*
            Parts eligible for declaration: a, b, pace, window
         */
        int position = 0;
        boolean open = false;
        int lastPosition = 0;

        int a = item.length % item.pace;

        for (int i=startingIndex; i<endingIndex; ++i) {
            position += links.get(i).component.length;
            if (position > item.pace) {
                //break;
            }

            // register a, b or pace
            if (position%item.pace==0 || position%item.pace==a) {
                if (open) {
                    register(position-lastPosition, lastPosition, item.pace, item.id);
                    open = false;
                }
                lastPosition = position;
            } else {
                open = true;
            }
        }

        // register window
        register(item.length, 0, item.pace, item.id);
    }


    public void organize () {

        links.clear();
        setUpBase();

        for (int i=0; i<windows.size()-1; ++i) {

            WindowItem item = windows.get(i);
            int startingIndex = links.size();
            construct(item);

            // register partial result
            partialAggregate(item, startingIndex, links.size());
        }
    }

}




class UseLink implements Serializable {
    String dest;
    ResultDeclaration component;
    int start; //position in the receiver
    int index; //the index of the component, which will be set later
    int pace; //receiving frequency

    UseLink (String dest, ResultDeclaration component, int start, int pace) {
        this.dest = dest;
        this.component = component;
        this.start = start;
        this.pace = pace;
    }

    UseLink (String dest, ResultDeclaration component, int start, int pace, int index) {
        this.dest = dest;
        this.component = component;
        this.start = start;
        this.pace = pace;
        this.index = index;
    }

    @Override
    public String toString() {
        return "["+ component.toString()+"]->\""+dest+"\" @ "+Integer.toString(start)+"|"+Integer.toString(pace);
    }
}

class ResultDeclaration implements Serializable {
    int length;
    int start;
    int pace;
    String windowId;

    ResultDeclaration(int length, int start, int pace, String windowId) {
        this.length = length;
        this.start = start;
        this.pace = pace;
        this.windowId = windowId;
    }

    @Override
    public String toString() {
        return "\""+windowId+"\" @ "+Integer.toString(start)+"~"
                +Integer.toString(length)+"|"+Integer.toString(pace);
    }
}