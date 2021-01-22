//package edu.usfca.cs.mr.sentiment_analysis;
//
//import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.mapreduce.Reducer;
//
//import java.util.ArrayList;
//import java.util.Comparator;
//import java.util.Iterator;
//import java.util.concurrent.ConcurrentSkipListSet;
//
///**
// * Reducer: Input to the reducer is the output from the mapper. It receives
// * word, list<count> pairs.  Sums up individual counts per given word. Emits
// * <word, total count> pairs.
// */
//public class TagCountSAReducer
//extends Reducer<IntWritable, WeekSAWritable, IntWritable, WeekSAWritable> {
//
//    @Override
//    protected void reduce(
//            IntWritable key, Iterable<WeekSAWritable> values, Context context) {
//        try {
//            int count = 0;
//            WeekSAWritable wW = new WeekSAWritable();
//
////            HashMap<String, HashtagWritable> map = new HashMap<>()
//            ConcurrentSkipListSet<HashtagSAWritable> set = new ConcurrentSkipListSet<>((o1, o2) -> {
//                if(o1.getHashtag().equals(o2.getHashtag()))
//                    return 0;
//                if(o1.getCount().get() > o2.getCount().get())
//                    return -1;
//                return 1;
//            });
//
//            for(WeekSAWritable val : values){
//                for(HashtagSAWritable hw : val.getHashTagList()){
//                    if(set.size() == 0){
//                        set.add(hw);
//                    }
//                    else {
//                        boolean found = false;
//                        if (set.contains(hw)) {
//                            for (HashtagSAWritable h : set) {
//
//                                if (h.getHashtag().equals(hw.getHashtag())) {
//                                    h.setCount(new IntWritable(h.getCount().get() + hw.getCount().get()));
//                                    found = true;
//                                    break;
//                                }
//                            }
//                            if (!found)
//                                set.add(hw);
//                        }
//                        else
//                            set.add(hw);
//                    }
//                }
//
//                // calculate the total count
//                count++;
//            }
//            Iterator<HashtagSAWritable> itr = set.iterator();
//            ArrayList<HashtagSAWritable> arrayList = new ArrayList<>(5);
//            int idx = 0;
//            while (itr.hasNext()){
//                if(idx == 5)
//                    break;
//                arrayList.add(itr.next());
//                idx++;
//            }
//            // Top 5 trending hashtags for a week
//            wW.addAllHashTagList(arrayList);
//
//            wW.setCount(new IntWritable(count));
//            wW.setWeek(key);
//            context.write(key, wW);
//
//        }
//        catch (Exception e){
//            throw new Error(e);
//        }
//    }
//}
