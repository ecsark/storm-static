package storm.blueprint;

import org.junit.Test;

public class RemainderGroupTest {

    @Test
    public void testOrganize() throws Exception {
        RemainderGroup group = new RemainderGroup(5);
        String[] ids = new String[] {"8/5","12/5","13/10","17/10","28/20","33/10","32/20","38/20","48/40"};
        int[] len = new int[] {8,12,13,17,28,33,32,38,48};
        int[] pace = new int[] {5,5,10,10,20,10,20,20,40};
        for (int i=0; i<ids.length; ++i) {
            group.add(ids[i],len[i],pace[i]);
        }

        group.organize();
    }
}