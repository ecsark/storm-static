package storm.blueprint;

import storm.blueprint.util.Divisor;
import storm.blueprint.util.ListMap;

import java.io.Serializable;
import java.util.*;

/**
 * User: ecsark
 * Date: 5/31/14
 * Time: 9:39 PM
 */
public class RemainderGroup implements Serializable {

    List<WindowItem> windows;

    Set<Delegate> delegates;

    List<Delegate> bases;

    RootDelegate rootDelegate;

    int basePace;

    RemainderGroup (int basePace) {

        this.basePace = basePace;
        windows = new ArrayList<WindowItem>();
        bases = new ArrayList<Delegate>();
        rootDelegate = new RootDelegate(basePace);
        delegates = new TreeSet<Delegate>(new DelegateComparator());//descending order of pace
    }

    public void add(String id, int windowLength, int pace) {
        windows.add(new WindowItem(id, windowLength, pace));
    }

    public void merge(RemainderGroup another) {
        windows.addAll(another.windows);
    }

    // delegate should have the largest pace
    protected Delegate construct (WindowItem window, Delegate delegate) {

        Delegate currentDelegate = delegate;

        // if there is no parent delegate, set up a new base
        if (currentDelegate == null) {
            currentDelegate = new Delegate(basePace, window.length %basePace);
            bases.add(currentDelegate);
            delegates.add(currentDelegate);
        }

        int currentPace = currentDelegate.pace;
        int fold = window.pace/currentPace;

        if (fold > 1) {

            List<Integer> divisors = Divisor.getDivisors(fold);
            Collections.sort(divisors); //sort in ascending order

            for (int divisor : divisors) {
                currentPace *= divisor;
                Delegate dg = new Delegate(currentPace, window.length %currentPace);
                currentDelegate.addClient(dg);
                boolean success = delegates.add(dg);
                assert(success);//TODO: remove this
                currentDelegate = dg;
            }
        }

        currentDelegate.addEndClient(window);
        return currentDelegate;
    }


    private void shrink2 (Delegate delegate) {


        Iterator<Delegate> iter = delegate.clients.iterator();

        List<Delegate> newConnection = new ArrayList<Delegate>();

        while(iter.hasNext()) {
            Delegate childDelegate = iter.next();
            if (childDelegate.endClients.size()==0 &&
                    childDelegate.pace/delegate.pace==2 &&
                    childDelegate.clients.size()==1 &&
                    childDelegate.clients.get(0).pace/childDelegate.pace==2) {

                delegates.remove(childDelegate);
                newConnection.add(childDelegate.clients.get(0));
                iter.remove();
            } else {
                shrink2(childDelegate);
            }
        }

        for (Delegate nconnect : newConnection) {
            delegate.clients.add(nconnect);
            shrink2(nconnect);
        }

    }

    protected void shrink2 () {
        for (Delegate base: bases) {
            shrink2(base);
        }
    }

    public void organize() {

        delegates.clear();

        // add window to topology one by one
        for (WindowItem window : windows) {

            Iterator<Delegate> iter = delegates.iterator();
            boolean constructed = false;

            while (iter.hasNext()) {
                Delegate delegate = iter.next();
                if (window.pace%delegate.pace==0
                        &&window.length %delegate.pace==delegate.remainder) {

                    construct(window, delegate);
                    constructed = true;
                    break;
                }
            }

            if (!constructed)
                construct(window, null);
        }

        // add root
        // NOTE: rootDelegate is not in delegates!!!
        rootDelegate.clients.clear();
        for (Delegate base : bases) {
            rootDelegate.addClient(base);
        }
        rootDelegate.split();

        shrink2();

    }

    class DelegateComparator implements Comparator<Delegate>, Serializable {
        @Override
        public int compare(Delegate o1, Delegate o2) {
            if (o2.pace==o1.pace) {
                return o2.remainder - o1.remainder;
            }
            return o2.pace - o1.pace;
        }//descending order of pace
    }
}


class Delegate implements Serializable {
    int pace;
    int remainder;

    List<Delegate> clients;

    List<WindowItem> endClients; //window.length % pace = remainder

    Delegate () {
        endClients = new ArrayList<WindowItem>();
        clients = new ArrayList<Delegate>();
    }

    Delegate(int pace, int remainder) {
        this.pace = pace;
        this.remainder = remainder;
        endClients = new ArrayList<WindowItem>();
        clients = new ArrayList<Delegate>();
    }

    void addEndClient (WindowItem endClient) {
        endClients.add(endClient);
    }

    void addClient (Delegate client) {
        clients.add(client);
    }
}


class RootDelegate extends Delegate {

    Map<Integer, List<Delegate>> delegateMap;
    List<Integer> triggers;

    RootDelegate(int pace) {
        super();
        this.pace = pace;
        this.remainder = 0;
    }

    boolean sanityCheck() {
        for (Delegate delegate : clients) {
            if (delegate.pace != pace)
                return false;
        }
        return true;
    }

    void split () {

        assert(sanityCheck()); //TODO: remove assert

        ListMap<Integer, Delegate> dm = new ListMap<Integer, Delegate>(clients,
                new ListMap.KeyExtractable<Integer, Delegate>() {
            @Override
            public Integer getKey(Delegate item) {
                return item.remainder;
            }
        });

        delegateMap = dm.getMap();

        // remainder%pace=0 should be considered as remainder%pace=pace
        if (delegateMap.containsKey(0)) {
            delegateMap.put(pace, delegateMap.get(0));
            delegateMap.remove(0);
        } else {
            delegateMap.put(pace, new ArrayList<Delegate>());
        }

        triggers = new ArrayList<Integer>(delegateMap.keySet());
        Collections.sort(triggers);
    }

}

