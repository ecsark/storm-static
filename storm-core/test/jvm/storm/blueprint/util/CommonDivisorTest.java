package storm.blueprint.util;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class CommonDivisorTest {

    @Test
    public void testGetGreatestCommonDivisor() throws Exception {
        List<Integer> data = new ArrayList<Integer>(Arrays.asList(8,20,6));
        assertEquals(2, Divisor.getGreatestCommonDivisor(data));

        List<Integer> data2 = new ArrayList<Integer>(Arrays.asList(8,20,100));
        assertEquals(4, Divisor.getGreatestCommonDivisor(data2));
    }

    @Test
    public void testGetLeastCommonMultiple() throws Exception {
        List<Integer> data = new ArrayList<Integer>(Arrays.asList(8,20,6));
        assertEquals(120, Divisor.getLeastCommonMultiple(data));

    }

    @Test
    public void testBitSet() {
        BitSet bs = new BitSet();
        bs.set(0);bs.set(1);bs.set(2);
        byte[] ba = bs.toByteArray();
        System.out.println(bs.toByteArray());
    }
}