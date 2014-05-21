package storm.blueprint;

import java.util.ArrayList;
import java.util.List;

/**
 * User: ecsark
 * Date: 4/21/14
 * Time: 8:49 PM
 */
public class WindSpeedBolts {

    List<AutoBolt> bolts = new ArrayList<AutoBolt>();

    void setUpAVGBolts() {
        bolts = new ArrayList<AutoBolt>();

        int[] windowLength = new int[] {3, 60, 600};
        int[] pace = new int[] {1, 5, 10};

        for (int i=0; i<windowLength.length; ++i) {
            AutoBoltBuilder builder = new AutoBoltBuilder("avg_"+Integer.toString(i));
            try {
                AutoBolt bolt = builder
                        .setFunction("avg")
                        .setInputSelectFields("windspeed")
                        .setOutputFields("windspeed_avg_" + Integer.toString(windowLength[i]))
                        .setTupleBuffer("incremental", windowLength[i], pace[i])
                        .build();
                bolts.add(bolt);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        AutoBoltBuilder hbuilder = new AutoBoltBuilder("avg_"+Integer.toString(3));
        try {
            AutoBolt hBolt = hbuilder.setFunction("avg")
                    .setInputSelectFields("windspeed")
                    .setOutputFields("windspeed_avg_" + Integer.toString(windowLength[3]))
                    .setTupleBuffer("hier-incremental", 3600, 30)
                    .build();
            bolts.add(hBolt);
        }  catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    void setUpMAXBoltsNaive() {
        bolts = new ArrayList<AutoBolt>();

        int[] windowLength = new int[] {5, 10, 20, 60, 120, 360};
        int[] pace = new int[] {5, 5, 10, 10, 20, 20};

        for (int i=0; i<windowLength.length; ++i) {
            AutoBoltBuilder builder = new AutoBoltBuilder("max_"+Integer.toString(i));
            try {
                AutoBolt bolt = builder
                        .setFunction("max")
                        .setInputSelectFields("windspeed")
                        .setOutputFields("windspeed_max_" + Integer.toString(windowLength[i]))
                        .setTupleBuffer("full", windowLength[i], pace[i])
                        .build();
                bolts.add(bolt);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }



    }

}
