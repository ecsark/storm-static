package storm.blueprint;

import java.io.Serializable;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;

/**
 * User: ecsark
 * Date: 5/21/14
 * Time: 1:41 PM
 */
public class Timing implements Serializable{

    //TESTING
    boolean firstRun = true;
    long startTime,startTimeCPU;
    int counter=0;
    ThreadMXBean bean;
    long threadId;
    int count;
    int start;

    public Timing (int count) {
        this.count = count;
    }

    public Timing (int count, int start) {
        this.count = count;
        this.start = start;
    }

    public void beforeTest () {
        if (firstRun) {
            if (start > 0) {
                --start;
                return;
            }
            bean = ManagementFactory.getThreadMXBean();
            threadId = Thread.currentThread().getId();
            startTime = bean.getThreadUserTime(threadId);
            startTimeCPU = bean.getThreadCpuTime(threadId);
            System.out.println("Start time:"+startTime);
            firstRun = false;
        }
    }

    public void afterTest () {
        if (counter < count) {
            ++counter;
        } else if (counter==count) {
            long endTime = bean.getThreadUserTime(threadId);
            long endTimeCPU = bean.getThreadCpuTime(threadId);
            System.out.println("End time:"+endTime);
            System.out.println("User time:"+Long.toString(endTime-startTime));
            System.out.println("CPU time:"+Long.toString(endTimeCPU-startTimeCPU));
            ++counter;
        }
    }
}
