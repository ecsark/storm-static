package storm.blueprint;

import backtype.storm.tuple.Tuple;
import storm.blueprint.buffer.LibreTupleBuffer;
import storm.blueprint.buffer.TupleBuffer;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.List;

/**
 * User: ecsark
 * Date: 5/19/14
 * Time: 11:15 AM
 */
public class LibreEntranceBuffer extends TupleBuffer {

    LibreTupleBuffer buf;
    List<Integer> position;
    int size;

    //TESTING
    boolean firstRun = true;
    long startTime,startTimeCPU;
    int counter=0;
    ThreadMXBean bean;
    long threadId;

    private void beforeTest () {
        if (firstRun) {
            bean = ManagementFactory.getThreadMXBean();
            threadId = Thread.currentThread().getId();
            startTime = bean.getThreadUserTime(threadId);
            startTimeCPU = bean.getThreadCpuTime(threadId);
            System.out.println("Start time:"+startTime);
            firstRun = false;
        }
    }

    private void afterTest () {
        if (counter==1600) {
            long endTime = bean.getThreadUserTime(threadId);
            long endTimeCPU = bean.getThreadCpuTime(threadId);
            System.out.println("End time:"+endTime);
            System.out.println("User time:"+Long.toString(endTime-startTime));
            System.out.println("CPU time:"+Long.toString(endTimeCPU-startTimeCPU));
            ++counter;
        } else {
            ++counter;
        }
    }


    LibreEntranceBuffer(LibreTupleBuffer tupleBuffer) {
        this.buf = tupleBuffer;
        size = buf.getSize();
        position = new ArrayList<Integer>();
        position.add(0);
        buf.allowColdStart(false);
    }

    @Override
    public void put(Tuple tuple) {

        //TESTING
        beforeTest();

        buf.put(tuple, position);

        position.set(0, (position.get(0)+1)%size);

        //TESTING
        afterTest();
    }
}
