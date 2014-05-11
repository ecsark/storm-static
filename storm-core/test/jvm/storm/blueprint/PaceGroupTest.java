package storm.blueprint;

import junit.framework.TestCase;

public class PaceGroupTest extends TestCase {

    WindowManager wm;

    public void setUp() throws Exception {
        super.setUp();
        wm = new WindowManager();
        wm.put("28/5", 28, 5);
        wm.put("12/5", 12, 5);
    }

    public void testOrganize() throws Exception {
        wm.consolidate();
    }
}