package storm.blueprint;

import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import storm.blueprint.buffer.DelegateBuffer;
import storm.blueprint.buffer.FullBuffer;
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

    transient Map<Integer, PatternGroup> windows;

    PatternBoltBuilder() {
        super();
        windows = new HashMap<Integer, PatternGroup>();
    }


    @Override
    public PatternBoltBuilder addWindow(String id, int windowLength, int pace) {

        String windowName = uniqueWindowName(id);

        if (!windows.containsKey(pace))
            windows.put(pace, new PatternGroup(pace));
        PatternGroup patternGroup = windows.get(pace);

        patternGroup.add(windowName, windowLength, pace);
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

        for (PatternGroup patternGroup : windows.values()) {
            patternGroup.organize();
        }
    }

    private List<Integer> computeClientPartSize (Delegate delegate, int pace, int remainder) {

        List<Integer> clientPartSize = new ArrayList<Integer>();

        int firstPartSize = remainder/delegate.pace * 2;
        int secondPartSize = 0;
        int secondPartLength = pace - remainder;

        if (remainder%delegate.pace != 0) {
            firstPartSize += 1;
            secondPartSize += 1;
            secondPartLength -= delegate.pace - delegate.remainder;
        }

        secondPartSize += secondPartLength/delegate.pace * 2;
        if (secondPartLength%delegate.pace != 0) {
            secondPartSize += 1;
        }

        if (firstPartSize != 0)
            clientPartSize.add(firstPartSize);
        if (secondPartSize != 0)
            clientPartSize.add(secondPartSize);



        return clientPartSize;
    }


    private DelegateBuffer buildBuffer (Delegate delegate, List<Integer> partSize) {

        String id = Integer.toString(delegate.pace)+'/'+Integer.toString(delegate.remainder);

        DelegateBuffer buffer = new DelegateBuffer(id, partSize, delegate.pace);
        buffer.setEmitting(false);
        buffer.setSelectFields(inputFields);
        buffer.setFunction(function);
        buffers.add(buffer);

        // here we only allow a delegate to declare 2 parts
        assert(partSize.size() == 2);


        for (final WindowItem endClient : delegate.endClients) {

            int size = endClient.length/delegate.pace * 2;
            if (endClient.length%delegate.pace != 0)
                size += 1;

            // TODO: modify to accommodate more buffer types
            final FullBuffer windowBuffer = new FullBuffer(endClient.id, size, 2, endClient.length);
            windowBuffer.setSelectFields(inputFields);
            windowBuffer.setFunction(function);
            buffers.add(windowBuffer);

            windowBuffer.addCallback(new WindowResultCallback() {
                @Override
                public void process(Tuple tuple) {
                    bolt.getCollector().emit(endClient.id, new Values(tuple.getValues().get(0)));
                    //ResultWriter.write(bolt.getId(), endClient.id + ": " + tuple.getValues().get(0) + "\n");
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

        final List<DelegateBuffer> rootBuffers = new ArrayList<DelegateBuffer>();

        for (PatternGroup patternGroup : windows.values()) {

            RootDelegate rootDelegate = patternGroup.rootDelegate;
            String id = "root_"+ rootDelegate.pace;

            List<Integer> rootPartSize = new ArrayList<Integer>();
            rootPartSize.add(rootDelegate.triggers.get(0));
            for (int i=1; i< rootDelegate.triggers.size(); ++i) {
                rootPartSize.add(rootDelegate.triggers.get(i) - rootDelegate.triggers.get(i - 1));
            }

            DelegateBuffer rootBuffer = new DelegateBuffer(id, rootPartSize, rootDelegate.pace);
            rootBuffer.setEmitting(false);
            rootBuffer.setSelectFields(inputFields);
            rootBuffer.setFunction(function);
            rootBuffers.add(rootBuffer);

            int partNum = rootDelegate.triggers.size();

            //NOTE base and root have the same pace!
            for (int i=0; i< rootDelegate.triggers.size(); ++i) {

                List<Integer> partSize = new ArrayList<Integer>();

                partSize.add(i+1);
                if (partNum-i > 1) { //force last part
                    partSize.add(partNum - i - 1);
                }

                int trigger = rootDelegate.triggers.get(i);

                for (Delegate base : rootDelegate.delegateMap.get(trigger)) {

                    final DelegateBuffer baseBuffer = buildBuffer(base, partSize);
                    rootBuffer.addCallback(new WindowResultCallback() {
                        @Override
                        public void process(Tuple tuple) {
                            baseBuffer.put(tuple);
                        }
                    });
                }
            }
        }

        bolt.entrance = new IEntrance() {
            @Override
            public void put(Tuple tuple) {
                for (DelegateBuffer buffer : rootBuffers) {
                    buffer.put(tuple);
                }
            }
        };

        return bolt;
    }
}
