package storm.blueprint.util;

import java.io.Serializable;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.List;

/**
 * User: ecsark
 * Date: 5/21/14
 * Time: 1:41 PM
 */
public class Timer implements Serializable{

    //TESTING
    boolean firstRun = true;
    long startTime,startTimeCPU;
    int counter=0;
    ThreadMXBean bean;
    long threadId;
    int count;
    int start;

    List<TaskFinishedCallback> callbacks;

    public Timer(int count) {
        this.count = count;
        callbacks = new ArrayList<TaskFinishedCallback>();
    }

    public Timer addCallback (TaskFinishedCallback callback) {
        callbacks.add(callback);
        return this;
    }

    public Timer(int count, int start) {
        this(count);
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
            for (TaskFinishedCallback callback : callbacks) {
                callback.onTaskFinished();
            }
        }
    }

    public interface TaskFinishedCallback extends Serializable {
        void onTaskFinished ();
    }
}
