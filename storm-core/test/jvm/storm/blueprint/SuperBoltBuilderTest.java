package storm.blueprint;

import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class SuperBoltBuilderTest {

    SuperBoltBuilder builder;


    @Test
    public void testConsolidate() throws Exception {
        builder = new SuperBoltBuilder();
        builder.addWindow("82/20",82,20);
        builder.addWindow("122/60",122,60);
        builder.addWindow("150/90",150,90);
        Map<Integer, List<Integer>> topology = builder.makeTopology();
        assertEquals(5, topology.entrySet().size());
    }

    @Test
    public void testConsolidate2() throws Exception {
        builder = new SuperBoltBuilder();
        builder.addWindow("82/20",82,20);
        builder.addWindow("122/60",122,60);
        builder.addWindow("150/80",150,80);
        builder.addWindow("150/90",150,90);
        Map<Integer, List<Integer>> topology = builder.makeTopology();
        assertEquals(1, topology.get(20).size());
    }

    @Test
    public void testRemainderGrouping() throws Exception {
        builder = new SuperBoltBuilder();
        builder.addWindow("82/20",82,20);
        builder.addWindow("122/60",122,60);
        builder.addWindow("150/80",150,80);
        builder.addWindow("150/90",150,90);
        builder.build();
    }
}