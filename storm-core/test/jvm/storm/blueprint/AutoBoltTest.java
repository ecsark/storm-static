package storm.blueprint;

import backtype.storm.Config;
import backtype.storm.ILocalCluster;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import storm.blueprint.function.Max;
import storm.blueprint.function.Sum;
import storm.blueprint.util.Counter;
import storm.blueprint.util.ListMap;
import storm.blueprint.util.Timer;

import java.util.*;

public class AutoBoltTest {

    private static AutoBoltBuilder setupAutoBolt(AutoBoltBuilder builder, int[] length, int[] pace) {
        for (int i=0; i<length.length; ++i) {
            String id = Integer.toString(length[i])+"/"+Integer.toString(pace[i]);
            builder.addWindow(id , length[i], pace[i]);
        }
        return builder;
    }

    static AutoBolt setupBolt1(AutoBoltBuilder builder) {
        return builder.setFunction(new Max())
                .setInputFields(new Fields("windspeed"))
                .setOutputFields(new Fields("windspeed_max"))
                .addWindow("28/5",28,5)
                .addWindow("26/5",26,5)
                .addWindow("43/10",43,10)
                .addWindow("8/5",8,5)
                .addWindow("12/5",12,5)
                .addWindow("60/5",60,5)
                .addWindow("120/20",120,20)
                .build();
    }


    static AutoBolt setupBolt2(AutoBoltBuilder builder) {
        return builder.setFunction(new Sum())
                .setInputFields(new Fields("windspeed"))
                .setOutputFields(new Fields("windspeed_max"))
                .addWindow("5/5", 5, 5)
                .addWindow("40/5", 40, 5)
                .addWindow("90/5",90,5)
                .build();
    }

    static AutoBolt setupBolt3(AutoBoltBuilder builder) {
        return builder.setFunction(new Sum())
                .setInputFields(new Fields("windspeed"))
                .setOutputFields(new Fields("windspeed_max"))
                .addWindow("5/5", 5, 5)
                .addWindow("55/5", 55, 5)
                .addWindow("75/5",75,5)
                .addWindow("200/5",200,5)
                .addWindow("360/5", 360, 5)
                .addWindow("400/5", 400, 5)
                .addWindow("450/5", 450, 5)
                .addWindow("480/5",480,5)
                .addWindow("500/5", 500, 5)
                .addWindow("520/5", 500, 5)
                .addWindow("540/5", 540, 5)
                .addWindow("550/5", 500, 5)
                .build();
    }


    static AutoBolt setupBolt4(AutoBoltBuilder builder) {
        int [] length = new int[] {8,12,13,17,28,33,32,38,48};
        int [] pace = new int [] {5,5,10,10,20,30,20,20,40};
        return setupAutoBolt(builder, length, pace)
                .setFunction(new Max())
                .setInputFields(new Fields("windspeed"))
                .setOutputFields(new Fields("windspeed_max"))
                .build();
    }

    static AutoBolt setupUniformBolt(AutoBoltBuilder builder) {

        final Counter counter = new Counter();

        builder.setFunction(new Sum().setCounter(counter))
                .setInputFields(new Fields("windspeed"))
                .setOutputFields(new Fields("windspeed_sum"));

        int pace = 20;
        List<Integer> res = QueryGenerator.generate(5467, 200, pace, 1000); // seed, num, min, max


        System.out.println("===========Generated queries==========");

        for (int r : res) {
            builder.addWindow(r+"/"+pace, r, pace);
            System.out.print(r+"/"+pace+"\t");
        }

        System.out.println("\n===========End of generation==========");

        AutoBolt bolt = builder.build();
        if (builder.cells > 0) {
            System.out.println(builder.cells + " cells in total.");
        }

        return bolt.setTimer(new Timer(10000, 2000)
                .addCallback(new Timer.TaskFinishedCallback() {
            @Override
            public void onTaskFinished() {
                System.out.println("Aggregation counts: " + counter.getCount());
                //ResultWriter.closeAll();
                counter.setCount(0);
            }
        }));
    }


    static AutoBolt setupZipfBolt(AutoBoltBuilder builder) {

        final Counter counter = new Counter();

        builder.setFunction(new Sum().setCounter(counter))
                .setInputFields(new Fields("windspeed"))
                .setOutputFields(new Fields("windspeed_sum"));

        long seed = 5467;

        List<Integer> paces = QueryGenerator.generateZipf(seed, 800, 2, 400, 0.6); //seed, num, min, max, skew

        // pace -> length
        ListMap<Integer, Integer> queries = new ListMap<Integer, Integer>();

        Random rand = new Random((long)(1.0*seed*3.14159));
        for (int p : paces) {
            double overlap = rand.nextDouble()*50.0 + 1;
            int length = (int)(overlap * p);
            builder.addWindow(length+"/"+p, length, p);
            queries.put(p,length);
        }

        List<Integer> pacesList = new ArrayList<Integer>(queries.keySet());
        Collections.sort(pacesList);

        Set<Integer> paceSet = new TreeSet<Integer>(pacesList);
        for (int p : paceSet)
            System.out.print(p+"\t");
        System.out.println("--------------------------------------");

        System.out.println("===========Generated queries==========");
        for (int p : pacesList) {
            Collections.sort(queries.get(p));
            for (int length : queries.get(p)) {
                System.out.print(length+"/"+p+"\t");
            }
        }
        System.out.println("\n===========End of generation==========");


        AutoBolt bolt = builder.build();

        if (builder.cells > 0) {
            System.out.println(builder.cells + " cells in total.");
        }


        return bolt.setTimer(new Timer(30000, 20000)
                .addCallback(new Timer.TaskFinishedCallback() {
                    @Override
                    public void onTaskFinished() {
                        System.out.println("Aggregation counts: " + counter.getCount());
                        //ResultWriter.closeAll();
                        counter.setCount(0);
                    }
                }));
    }

    public static void main (String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        //builder.setSpout("spout", new TestSpout(), 1);
        builder.setSpout("spout", new WindSpeedSpout(), 1);


        //builder.setBolt("weave", setupBolt1(new WeaveBoltBuilder()), 1).shuffleGrouping("spout");

        /*************** UNIFORM ***************/
        //builder.setBolt("pattern", setupUniformBolt(new PatternBoltBuilder()), 1).shuffleGrouping("spout");
        //builder.setBolt("libre", setupUniformBolt(new LibreBoltBuilder()), 1).shuffleGrouping("spout");
        //builder.setBolt("naive", setupUniformBolt(new NaiveBoltBuilder()), 1).shuffleGrouping("spout");
        //builder.setBolt("super", setupUniformBolt(new SuperBoltBuilder()), 1).shuffleGrouping("spout");
        //builder.setBolt("weave", setupUniformBolt(new WeaveBoltBuilder()), 1).shuffleGrouping("spout");

        /**************** ZIPF ****************/
        //builder.setBolt("pattern", setupZipfBolt(new PatternBoltBuilder()), 1).shuffleGrouping("spout");
        //builder.setBolt("libre", setupZipfBolt(new LibreBoltBuilder()), 1).shuffleGrouping("spout");
        //builder.setBolt("naive", setupZipfBolt(new NaiveBoltBuilder()), 1).shuffleGrouping("spout");
        builder.setBolt("super", setupZipfBolt(new SuperBoltBuilder()), 1).shuffleGrouping("spout");
        //builder.setBolt("weave", setupZipfBolt(new WeaveBoltBuilder()), 1).shuffleGrouping("spout");


        Config conf = new Config();
        conf.setDebug(false);

        ILocalCluster cluster = new LocalCluster();
        cluster.submitTopology("windspeed", conf, builder.createTopology());

        Thread.sleep(200000);

        cluster.shutdown();

    }

}