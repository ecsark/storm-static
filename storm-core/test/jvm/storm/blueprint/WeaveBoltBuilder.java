package storm.blueprint;

import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.PrimitiveSink;
import storm.blueprint.buffer.DelegateBuffer;
import storm.blueprint.buffer.WeaveBuffer;
import storm.blueprint.buffer.IEntrance;
import storm.blueprint.buffer.WindowResultCallback;
import storm.blueprint.util.Divisor;

import java.util.*;

/**
 * User: ecsark
 * Date: 7/12/14
 * Time: 1:24 AM
 */
public class WeaveBoltBuilder extends AutoBoltBuilder {

    transient List<Slice> slices = new ArrayList<Slice>();

    private void addWindow (WindowItem window) {
        slices.add(new Slice(window));
    }

    private void consolidate () {

        double inputRate = 1.0;

        double[][] costSaving = new double[slices.size()][slices.size()];
        for (int i=0; i<slices.size(); ++i) {
            for (int j=0; j<slices.size(); ++j) {
                double originalCost = slices.get(i).getCost() + slices.get(j).getCost();
                double updatedCost = slices.get(i).tryAddWindow(slices.get(j).windows);
                costSaving[i][j] = costSaving[j][i] = originalCost - updatedCost + inputRate;
            }
        }

        Set<Integer> removedSlices = new HashSet<Integer>();

        while (true) {
            double maxBenefit = -1.0;
            int a = -1, b = -1;
            for (int i=0; i<slices.size()-1; ++i) {

                if (removedSlices.contains(i))
                    continue;

                for (int j=i+1; j<slices.size(); ++j) {
                    if (removedSlices.contains(j))
                        continue;

                    double benefit = costSaving[i][j];
                    if (benefit > maxBenefit) {
                        maxBenefit = benefit;
                        a = i; b = j;
                    }
                }
            }

            if (maxBenefit > 0) {
                slices.get(a).addWindow(slices.get(b).windows);
                removedSlices.add(b);

                for (int i=0; i<slices.size(); ++i) {
                    if (removedSlices.contains(i) || i==a)
                        continue;

                    double originalCost = slices.get(a).getCost() + slices.get(i).getCost();
                    //double updatedCost = slices.get(a).tryAddWindow(slices.get(i).windows);
                    double updatedCost = slices.get(a).tryMergeSlice(slices.get(i));
                    costSaving[i][a] = costSaving[a][i] = originalCost - updatedCost + inputRate;
                }
            } else {
                break;
            }
        }

        Iterator<Slice> sIter = slices.iterator();
        int idx = 0;
        while (sIter.hasNext()) {
            sIter.next();
            if (removedSlices.contains(idx))
                sIter.remove();
            idx++;
        }

        System.out.println("Number of trees: " + slices.size());
    }

    @Override
    public AutoBoltBuilder addWindow(String id, int length, int pace) {
        String uniqueId = uniqueWindowName(id);
        addWindow(new WindowItem(uniqueId, length, pace));
        return this;
    }



    @Override
    public AutoBolt build() {
        consolidate();

        final List<DelegateBuffer> delegates = new ArrayList<DelegateBuffer>();

        int i = 0;
        for (Slice s : slices) {
            String id = "__delegate_"+s.windows.size()+"_"+s.lcm+"_"+i++;
            List<Integer> partSize = new ArrayList<Integer>();
            // TODO:
            int prevj = 0;
            for (int j=s.map.nextSetBit(0); j>=0; j=s.map.nextSetBit(j+1)) {
                partSize.add(j-prevj);
                prevj = j;
            }

            DelegateBuffer delegate = new DelegateBuffer(id, partSize, s.lcm);
            delegate.setEmitting(false);
            delegate.setFunction(function);
            delegate.setSelectFields(inputFields);

            delegates.add(delegate);

            for (WindowItem window : s.windows) {
                List<Integer> parts = new ArrayList<Integer>();

                int a = window.length % window.pace;
                for (int j=s.map.nextSetBit(0), block=1; j>=0; j=s.map.nextSetBit(j+1), block++) {
                    if (j%window.pace==0 || j%window.pace==a) {
                        parts.add(block);
                    }
                }

                final WeaveBuffer buffer = new WeaveBuffer(window.id, window.pace, window.length, parts, s.lcm);
                buffer.setEmitting(true);
                buffer.setSelectFields(inputFields);
                buffer.setFunction(function);
                buffer.addCallback(new WindowResultCallback() {
                    @Override
                    public void process(Tuple tuple) {
                        bolt.getCollector().emit(buffer.getId(), new Values(tuple.getValues().get(0)));
                    }
                });

                delegate.addCallback(new WindowResultCallback() {
                    @Override
                    public void process(Tuple tuple) {
                        buffer.put(tuple);
                    }
                });
            }
        }

        bolt.entrance = new IEntrance() {
            @Override
            public void put(Tuple tuple) {
                for (DelegateBuffer delegate : delegates)
                    delegate.put(tuple);
            }
        };


        return bolt;
    }
}


class Slice {

    BitSet map = new BitSet();

    List<WindowItem> windows = new ArrayList<WindowItem>();

    int lcm = 1;
    double cachedCost = -1;
    double overlap = 0.0;

    Slice () {}

    Slice (WindowItem item) {
       addWindow(item);
    }


    void update(int pace, int offset) {
        while (offset <= lcm) {
            map.set(offset);
            offset += pace;
        }

    }

    void updateAll() {

        overlap = 0.0;

        for (WindowItem window : windows) {
            int a = window.length % window.pace;
            //int b = window.pace - a;
            update(window.pace, a);
            update(window.pace, window.pace);
            overlap += (double)window.length/window.pace;
        }

        map.clear(0);
        map.set(lcm);
        cachedCost = -1;
    }

    Slice reproduce() {
        Slice s = new Slice();

        s.map = (BitSet) map.clone();
        s.windows.addAll(this.windows);
        s.lcm = this.lcm;
        return s;
    }

    void addWindow(WindowItem item) {
        lcm = (int) Divisor.getLeastCommonMultiple(item.pace, lcm);
        windows.add(item);

        updateAll();
    }

    double tryAddWindow(WindowItem item) {
        Slice s = reproduce();
        s.addWindow(item);
        return s.getCost();
    }

    void addWindow(Collection<WindowItem> items) {
        for (WindowItem item : items) {
            lcm = (int) Divisor.getLeastCommonMultiple(item.pace, lcm);
            windows.add(item);
        }

        updateAll();
    }

    double tryAddWindow(Collection<WindowItem> items) {
        Slice s = reproduce();
        s.addWindow(items);
        return s.getCost();
    }


    double tryMergeSlice(Slice another) {

        long LCM = Divisor.getLeastCommonMultiple(this.lcm, another.lcm);

        Slice a, b;
        if (this.map.cardinality() < another.map.cardinality()) {
            a = this; b = another;
        } else {
            a = another; b = this;
        }

        BloomFilter<Long> bloomFilter = BloomFilter.create(new Funnel<Long>() {
            @Override
            public void funnel(Long from, PrimitiveSink into) {
                into.putLong(from);
            }
        }, (int)(LCM/a.lcm)*a.map.cardinality(), 0.01);


        for (int i=0; i<LCM/a.lcm; ++i) {
            long base = i * a.lcm;
            for (int j=a.map.nextSetBit(0); j>=0; j=a.map.nextSetBit(j+1)) {
                bloomFilter.put(base + j);
            }
        }

        long common = 0;
        for (int i=0; i<LCM/b.lcm; ++i) {
            long base = i * b.lcm;
            for (int j=b.map.nextSetBit(0); j>=0; j=b.map.nextSetBit(j+1)) {
                if (bloomFilter.mightContain(base+j))
                    common++;
            }
        }

        double edgeRate = (double)(LCM/this.lcm*this.map.cardinality()
                +LCM/another.lcm*another.map.cardinality() - common)/LCM;

        return (this.overlap+another.overlap) * edgeRate;

    }


    double getCost() {
        if (cachedCost > 0)
            return cachedCost;
        double edgeRate = (double) map.cardinality() / lcm;
        cachedCost =  edgeRate * overlap;
        return cachedCost;
    }
}