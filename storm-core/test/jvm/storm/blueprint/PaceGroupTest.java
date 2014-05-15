package storm.blueprint;

import junit.framework.TestCase;

import java.util.Collection;
import java.util.List;

public class PaceGroupTest extends TestCase {

    WindowManager wm;
    Collection<PaceGroup> paceGroups;

    public void setUp() throws Exception {
        super.setUp();
        wm = new WindowManager();
        addWindow(28, 5);
        //addWindow(27, 5);
        //addWindow(43, 5);
        //addWindow(8, 5);
        addWindow(12, 5);
        //addWindow(26, 5);

        paceGroups = wm.windows.values();
    }

    public void setUp1() throws Exception {
        super.setUp();
        wm = new WindowManager();
        addWindow(30, 5);
        addWindow(10, 5);
        addWindow(600, 30);
        addWindow(45, 5);

        paceGroups = wm.windows.values();
    }

    private void addWindow (int length, int pace) {
        wm.put(length+"/"+pace, length, pace);
    }

    public void testOrganize() throws Exception {
        wm.consolidate();

        System.out.println("Result Declarations:");
        printDeclarations();

        System.out.println("\nPace Group:");
        printUseLinks();

    }


    private void printDeclarations () {
        for (PaceGroup pg : paceGroups) {
            for (List<ResultDeclaration> decList : pg.registry.values()) {
                for (ResultDeclaration declaration : decList) {
                    System.out.println(declaration);
                }
            }
        }
    }

    private void printUseLinks() {
        for (PaceGroup pg : paceGroups) {
            double cpu = 0;
            for (UseLink link : pg.links) {
                System.out.println(link);
                cpu += 1.0/link.pace;
            }
            System.out.println("CPU: "+cpu+"/per second");

        }
    }
}