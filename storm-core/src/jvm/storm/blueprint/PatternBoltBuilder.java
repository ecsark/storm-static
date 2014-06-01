package storm.blueprint;

import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import storm.blueprint.buffer.DelegateBuffer;
import storm.blueprint.buffer.FullWindowBuffer;
import storm.blueprint.buffer.IEntrance;
import storm.blueprint.buffer.WindowResultCallback;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * User: ecsark
 * Date: 5/31/14
 * Time: 9:38 PM
 */
public class PatternBoltBuilder extends AutoBoltBuilder {

    Map<Integer, RemainderGroup> windows;

    PatternBoltBuilder() {
        super();
        windows = new HashMap<Integer, RemainderGroup>();
    }


    @Override
    public PatternBoltBuilder addWindow(String id, int windowLength, int pace) {

        String windowName = uniqueWindowName(id);

        if (!windows.containsKey(pace))
            windows.put(pace, new RemainderGroup(pace));
        RemainderGroup remainderGroup = windows.get(pace);

        remainderGroup.add(windowName, windowLength, pace);
        return this;
    }

    protected void consolidate () {
        List<Integer> markToDelete = new ArrayList<Integer>();

        for (int pace : windows.keySet()) {
            for (int p : windows.keySet()) {
                if (pace%p==0 && pace>p) {
                    windows.get(p).merge(windows.get(pace));
                    markToDelete.add(pace);
                    break;
                }
            }
        }

        for (int pace : markToDelete) {
            windows.remove(pace);
        }

        for (RemainderGroup remainderGroup : windows.values()) {
            remainderGroup.organize();
        }
    }

    private List<Integer> computeClientPartSize (Delegate delegate, int pace, int remainder) {
        List<Integer> clientPartSize = new ArrayList<Integer>();

        int firstPartSize = remainder/delegate.pace * 2;
        int secondPartLength = pace - remainder;

        if (remainder%delegate.pace != 0) {
            firstPartSize += 1;
            secondPartLength -= delegate.remainder;
        }

        int secondPartSize = secondPartLength/delegate.pace * 2;
        if (secondPartLength%delegate.pace != 0) {
            secondPartSize += 1;
        }

        clientPartSize.add(firstPartSize);
        clientPartSize.add(secondPartSize);

        return clientPartSize;
    }


    private DelegateBuffer buildBuffer (Delegate delegate, List<Integer> partSize) {

        String id = Integer.toString(delegate.pace)+'/'+Integer.toString(delegate.remainder);

        DelegateBuffer buffer = new DelegateBuffer(id, partSize, delegate.pace);
        buffer.setEmitting(false);
        buffers.add(buffer);

        // here we only allow a delegate to declaring 2 parts
        assert(partSize.size() == 2);


        for (final WindowItem endClient : delegate.endClients) {

            int size = endClient.length/delegate.pace * 2;
            if (endClient.length%delegate.pace != 0)
                size += 1;

            // TODO: modify to accommodate more buffer types
            final FullWindowBuffer windowBuffer = new FullWindowBuffer(endClient.id, size, 2, endClient.length);
            buffers.add(windowBuffer);

            windowBuffer.addCallback(new WindowResultCallback() {
                @Override
                public void process(Tuple tuple) {
                    bolt.getCollector().emit(endClient.id, new Values(tuple.getValues().get(0)));
                }
            });

            buffer.addCallback(new WindowResultCallback() {
                @Override
                public void process(Tuple tuple) {
                    windowBuffer.put(tuple);
                }
            });
        }

        for (Delegate client : delegate.clients) {

            List<Integer> clientPartSize = computeClientPartSize(delegate, client.pace, client.remainder);

            final DelegateBuffer clientBuffer = buildBuffer(client, clientPartSize);
            buffers.add(clientBuffer);

            buffer.addCallback(new WindowResultCallback() {
                @Override
                public void process(Tuple tuple) {
                    clientBuffer.put(tuple);
                }
            });
        }

        return buffer;
    }

    @Override
    public AutoBolt build () {
        consolidate();
        buffers.clear();

        final List<DelegateBuffer> baseBuffers = new ArrayList<DelegateBuffer>();

        for (RemainderGroup remainderGroup : windows.values()) {

            BaseDelegate baseDelegate = remainderGroup.baseDelegate;
            String id = "base_"+baseDelegate.pace;

            List<Integer> basePartSize = new ArrayList<Integer>();
            basePartSize.add(baseDelegate.triggers.get(0));
            for (int i=1; i<baseDelegate.triggers.size(); ++i) {
                basePartSize.add(baseDelegate.triggers.get(i) - baseDelegate.triggers.get(i-1));
            }

            DelegateBuffer baseBuffer = new DelegateBuffer(id, basePartSize, baseDelegate.pace);
            baseBuffer.setEmitting(false);
            baseBuffers.add(baseBuffer);

            int partNum = baseDelegate.triggers.size();

            if (!baseDelegate.triggers.contains(0)) {
                partNum += 1;
            }

            //NOTE entrance and base have the same pace!
            for (int trigger : baseDelegate.triggers) {

                List<Integer> partSize = new ArrayList<Integer>();

                partSize.add(trigger);
                if (partNum - trigger!=0) {
                    partSize.add(partNum - trigger);
                }

                for (Delegate entrance : baseDelegate.delegateMap.get(trigger)) {

                    final DelegateBuffer entranceBuffer = buildBuffer(entrance, partSize);
                    buffers.add(entranceBuffer);
                    baseBuffer.addCallback(new WindowResultCallback() {
                        @Override
                        public void process(Tuple tuple) {
                            entranceBuffer.put(tuple);
                        }
                    });
                }
            }
        }

        bolt.entrance = new IEntrance() {
            @Override
            public void put(Tuple tuple) {
                for (DelegateBuffer buffer : baseBuffers) {
                    buffer.put(tuple);
                }
            }
        };

        return bolt;
    }
}
