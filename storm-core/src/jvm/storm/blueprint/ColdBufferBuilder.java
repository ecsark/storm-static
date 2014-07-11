package storm.blueprint;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import storm.blueprint.buffer.ColdBuffer;
import storm.blueprint.buffer.LibreBuffer;
import storm.blueprint.buffer.WindowResultCallback;
import storm.blueprint.function.Functional;
import storm.blueprint.util.ListMap;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * User: ecsark
 * Date: 7/7/14
 * Time: 11:51 AM
 */
public class ColdBufferBuilder implements Serializable {

    private class ColdLink implements Serializable{
        UseLink use;
        int transfers;
        int trigger;
        int destIndex;

        ColdLink(UseLink use, int transfers, int trigger, int destIndex) {
            this.use = use;
            this.transfers = transfers;
            this.trigger = trigger;
            this.destIndex = destIndex;
        }
    }


    private List<ColdLink> computeColdLinks(List<UseLink> partition, List<Integer> ancestorStates) {

        int pace = partition.get(0).pace;

        List<ColdLink> coldLinks = new ArrayList<ColdLink>();

        int ancestorLayers = ancestorStates.size();
        if (ancestorLayers == 0)
            return coldLinks;

        List<Integer> ends = new ArrayList<Integer>(partition.size());
        for (UseLink link : partition)
            ends.add(link.component.length);
        for (int i=1; i<partition.size(); ++i)
            ends.set(i, ends.get(i)+ends.get(i-1));

        int progress = ancestorStates.get(ancestorStates.size()-1) + 1;
        for (int ancestorIndex=0; ancestorIndex<ancestorStates.size(); ++ancestorIndex) {

            for (int partIndex=ancestorStates.get(ancestorIndex); partIndex<progress; ++partIndex) {
                ResultDeclaration part =  partition.get(partIndex).component;
                int receiverEnd = ends.get(partIndex) - (ancestorLayers-ancestorIndex)*pace;
                int senderEnd = part.start + part.length;

                if (receiverEnd < senderEnd) { // cold start
                    int receiverStart = receiverEnd - part.length;
                    int newSenderStart = part.start % part.pace;
                    int trigger = (receiverStart - newSenderStart) / part.pace;

                    int transfers = 0;
                    while (ancestorIndex+1 < ancestorStates.size() && receiverEnd<senderEnd) {
                        transfers++;
                        receiverEnd += pace;
                    }

                    coldLinks.add(new ColdLink(partition.get(partIndex), transfers, trigger, partIndex));
                }
            }

            progress = ancestorStates.get(ancestorIndex);
        }

        return coldLinks;
    }


    public List<ColdBuffer> build (Map<String, LibreBuffer> buffers,
                                   Map<String, List<UseLink>> partitions,
                                   Functional function,
                                   Fields selectField) {

        List<ColdLink> allColdLinks = new ArrayList<ColdLink>();

        for (String id : buffers.keySet())
            allColdLinks.addAll(computeColdLinks(partitions.get(id), buffers.get(id).getAncestorStates()));

        ListMap<ResultDeclaration, ColdLink> decMap = new ListMap<ResultDeclaration, ColdLink>(
                allColdLinks, new ListMap.KeyExtractable<ResultDeclaration, ColdLink>() {
            @Override
            public ResultDeclaration getKey(ColdLink item) {
                return item.use.component;
            }
        });

        List<ColdBuffer> coldBuffers = new ArrayList<ColdBuffer>();

        for (List<ColdLink> components : decMap.getMap().values()) {
            ResultDeclaration declaration = components.get(0).use.component;

            int start = declaration.start % declaration.pace;
            int size = start + declaration.length;

            ColdBuffer buffer = new ColdBuffer("__cold"+declaration.toString(), size, declaration.pace, start);
            buffer.setFunction(function);
            buffer.setEmitting(false);
            buffer.setSelectFields(selectField);
            coldBuffers.add(buffer);

            for (final ColdLink component : components) {

                UseLink link = component.use;

                final int trigger = component.trigger;
                final int cycle = link.pace / link.component.pace;
                final int transfers = component.transfers;
                final List<Integer> destComponentIndices = new ArrayList<Integer>();
                destComponentIndices.add(component.destIndex);

                final LibreBuffer buf = buffers.get(link.dest);

                buffer.addCallback(new WindowResultCallback() {

                    int count = 0, step = 0;

                    @Override
                    public void process(Tuple tuple) {
                        if  (count < transfers) {
                            if (step == trigger) {
                                buf.put(tuple, destComponentIndices);
                                count++;
                            }
                            step = (step + 1) % cycle;
                        }
                    }
                });

                // minimum partial aggregate counts for the coldBuffer
                buffer.register((transfers-1)*cycle + trigger + 1);
            }
        }

        return coldBuffers;
    }
}
