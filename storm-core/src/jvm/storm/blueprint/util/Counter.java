package storm.blueprint.util;

import java.io.Serializable;

/**
 * User: ecsark
 * Date: 6/6/14
 * Time: 10:45 AM
 */
public class Counter implements Serializable {

    long counter = 0;

    public void increment (int num) {
        if (running)
            counter += num;
    }

    public long getCount () {
        return counter;
    }

    public void setCount (long counter) {
        this.counter = counter;
    }


    static boolean running = false;

    public static void setRun (boolean isRunnable)  {
        running = isRunnable;
    }
}
