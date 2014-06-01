package storm.blueprint;

import backtype.storm.tuple.Fields;
import org.junit.Test;
import storm.blueprint.buffer.LibreBuffer;
import storm.blueprint.function.Max;

import java.util.Collection;

public class LibreBufferBuilderTest {

    LibreBufferBuilder builder = new LibreBufferBuilder();

    LibreBoltBuilder wm;
    Collection<PaceGroup> paceGroups;

    private void addWindow (int length, int pace) {
        wm.addWindow(length + "/" + pace, length, pace);
    }


    public void setUp() throws Exception {
        wm = new LibreBoltBuilder();
        addWindow(28, 5);
        addWindow(27, 5);
        addWindow(43, 10);
        addWindow(8, 5);
        addWindow(12, 5);
        addWindow(26, 5);

        paceGroups = wm.windows.values();
        wm.consolidate();
    }

    @Test
    public void testBuild() throws Exception {
        setUp();
        Collection<LibreBuffer> tupleBuffers =
                builder.build(paceGroups.iterator().next(), new Max(),
                        new Fields("windspeed"), new AutoBolt());
    }
}