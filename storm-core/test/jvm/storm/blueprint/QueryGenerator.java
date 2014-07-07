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

    static List<Integer> generate (long seed, int num, int min, int max) {
        Random _rand;
        _rand = new Random(seed);
        List<Integer> res = new ArrayList<Integer>();
        for (int i=0; i<num; ++i)
            res.add(_rand.nextInt(max-min-1)+1+min);

        Collections.sort(res);
        //print (res);

        return res;

    }

    static List<Integer> generateZipf (long seed, int num, int min, int max, double skew) {
        ZipfGenerator zipf = new ZipfGenerator((max-min+1), skew, seed);
        List<Integer> res = new ArrayList<Integer>();
        for (int i=0; i<num; ++i)
            res.add(zipf.nextInt()+min);

        Collections.sort(res);
        //print (res);

        return res;
    }

    static void print (List<Integer> res) {
        System.out.println("==========Generated queries=========");
        for (int r : res) {
            System.out.print(r+"\t");
        }
        System.out.println("\n===========End of generation==========");
    }



}

class ZipfGenerator {
    private Random rnd;
    private int size;
    private double skew;
    private double bottom = 0;

    public ZipfGenerator(int size, double skew, long seed) {
        this.size = size;
        this.skew = skew;
        this.rnd = new Random(seed);

        for(int i=1;i < size; i++) {
            this.bottom += (1/Math.pow(i, this.skew));
        }
    }

    // the next() method returns an random rank id.
    // The frequency of returned rank ids are follows Zipf distribution.
    public int nextInt() {
        int rank;
        double frequency = 0;
        double dice;

        rank = rnd.nextInt(size);
        frequency = (1.0d / Math.pow(rank, this.skew)) / this.bottom;
        dice = rnd.nextDouble();

        while(!(dice < frequency)) {
            rank = rnd.nextInt(size);
            frequency = (1.0d / Math.pow(rank, this.skew)) / this.bottom;
            dice = rnd.nextDouble();
        }

        return rank;
    }

    // This method returns a probability that the given rank occurs.
    public double getProbability(int rank) {
        return (1.0d / Math.pow(rank, this.skew)) / this.bottom;
    }
}