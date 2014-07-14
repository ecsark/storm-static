package storm.blueprint;

import storm.blueprint.util.ListMap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * User: ecsark
 * Date: 7/5/14
 * Time: 12:50 PM
 */
public class Registry {

    private static Registry _instance;

    private Registry() {
        declarations = new HashMap<Integer, ListMap<Integer, ResultDeclaration>>();
        links = new ArrayList<UseLink>();
    }

    public static Registry instance() {
        if (_instance == null) {
            _instance = new Registry();
        }
        return _instance;
    }

    // pace->start->(pace,start,length,id)
    private Map<Integer, ListMap<Integer, ResultDeclaration>> declarations;
    private List<UseLink> links;

    public static void declare (ResultDeclaration declaration) {

        if (!instance().declarations.containsKey(declaration.pace)) {
            instance().declarations.put(declaration.pace, new ListMap<Integer, ResultDeclaration>());
        }
        ListMap<Integer, ResultDeclaration> paceMap = instance().declarations.get(declaration.pace);

        int key = declaration.start%declaration.pace;

        // avoid duplicates of <pace, start%pace, length>
        if (paceMap.containsKey(key)) {
            for (ResultDeclaration d : paceMap.get(key)) {
                if (d.length == declaration.length)
                    return;
            }
        }

        paceMap.put(key, declaration);
    }

    public static void reuse (UseLink useLink) {
        instance().links.add(useLink);
    }

    public static List<ResultDeclaration> findExact (int pace, int start, int max) {
        List<ResultDeclaration> results = new ArrayList<ResultDeclaration>();
        if (!instance().declarations.containsKey(pace) ||
                !instance().declarations.get(pace).containsKey(start))
            return results;

        for (ResultDeclaration d : instance().declarations.get(pace).get(start)) {
            if (d.length <= max)
                results.add(d);
        }
        return results;
    }

    public static List<ResultDeclaration> find (int pace, int start, int max) {
        return find (pace, start, max, false);
    }

    public static List<ResultDeclaration> find (int pace, int start, int max, boolean allowUnit) {
        List<ResultDeclaration> results = new ArrayList<ResultDeclaration>();

        Map<Integer, ListMap<Integer, ResultDeclaration>> reg = instance().declarations;

        for (int p : instance().declarations.keySet()) {
            if (pace % p == 0) {
                if (reg.get(p).containsKey(start%p)) {
                    for (ResultDeclaration d : reg.get(p).get(start%p)) {
                        if (d.length <= max) { // unit will not be used!!
                            if (!allowUnit && d.windowId.startsWith("UNIT"))
                                continue;
                            results.add(d);

                        }

                    }
                }
            }
        }

        return results;
    }

    public static ResultDeclaration getLongest (List<ResultDeclaration> results) {
        int maxLength = -1;
        ResultDeclaration result = null;
        for (ResultDeclaration dec : results) {
            if (dec.length > maxLength) {
                maxLength = dec.length;
                result = dec;
            }
        }
        return result;
    }

    public static List<UseLink> getLinks () {
        return instance().links;
    }
}




