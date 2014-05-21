package storm.blueprint;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

/**
 * User: ecsark
 * Date: 5/21/14
 * Time: 4:33 PM
 */
public class QueryGenerator {

    Random rand;

    QueryGenerator () {
        rand = new Random();
    }

    QueryGenerator (long seed) {
        rand = new Random(seed);
    }

    int next () {
        return rand.nextInt();
    }

    static List<Integer> generate (long seed, int num, int max) {
        Random _rand;
        _rand = new Random(seed);
        List<Integer> res = new ArrayList<Integer>();
        for (int i=0; i<num; ++i)
            res.add(_rand.nextInt(max-1)+1);

        Collections.sort(res);
        print (res);

        return res;

    }

    static void print (List<Integer> res) {
        System.out.println("==========Generated queries=========");
        for (int r : res) {
            System.out.print(r+"\t");
        }
        System.out.println("===========End of generation==========");
    }


}
