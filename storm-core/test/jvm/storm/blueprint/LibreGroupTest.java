package storm.blueprint;

import junit.framework.TestCase;

import java.util.Collection;
import java.util.List;

public class LibreGroupTest extends TestCase {

    LibreBoltBuilder wm;
    Collection<LibreGroup> libreGroups;


    public void setUp() throws Exception {
        super.setUp();
        wm = new LibreBoltBuilder();
        addWindow(8, 5);
        addWindow(12, 5);
        addWindow(13, 10);
        addWindow(17, 10);
        addWindow(28, 20);
        addWindow(33, 10);
        addWindow(32, 20);
        addWindow(38, 20);
        addWindow(48, 40);
        libreGroups = wm.windows.values();
    }

    public void setUp2() throws Exception {
        super.setUp();
        wm = new LibreBoltBuilder();
        addWindow(28, 5);
        //addWindow(27, 5);
        //addWindow(43, 5);
        addWindow(8, 5);
        addWindow(12, 5);
        //addWindow(26, 5);

        libreGroups = wm.windows.values();
    }

    public void setUp1() throws Exception {
        super.setUp();
        wm = new LibreBoltBuilder();
        addWindow(30, 5);
        addWindow(10, 5);
        addWindow(600, 30);
        addWindow(45, 5);

        libreGroups = wm.windows.values();
    }

    private void addWindow (int length, int pace) {
        wm.addWindow(length+"/"+pace, length, pace);
    }

    public void testOrganize() throws Exception {
        wm.consolidate();

        System.out.println("Result Declarations:");
        printDeclarations();

        System.out.println("\nPace Group:");
        printUseLinks();

    }


    private void printDeclarations () {
        for (LibreGroup pg : libreGroups) {
            for (List<ResultDeclaration> decList : pg.registry.values()) {
                for (ResultDeclaration declaration : decList) {
                    System.out.println(declaration);
                }
            }
        }
    }

    private void printUseLinks() {
        for (LibreGroup pg : libreGroups) {
            double cpu = 0;
            for (UseLink link : pg.links) {
                System.out.println(link);
                cpu += 1.0/link.pace;
            }
            System.out.println("CPU: "+cpu+"/per second");

        }
    }
}