package edu.usfca.cs.mr.stack_overflow_top5_posts;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * Reducer: Input to the reducer is the output from the mapper. It receives
 * word, list<count> pairs.  Sums up individual counts per given word. Emits
 * <word, total count> pairs.
 */
public class PostCountReducer
extends Reducer<IntWritable, WeeklyWritable, Text, WeeklyWritable> {

    private static Logger logger = LoggerFactory.getLogger(PostCountReducer.class.getName());

    @Override
    protected void reduce(
            IntWritable key, Iterable<WeeklyWritable> values, Context context)
    throws IOException, InterruptedException {
        try {

            TreeMap<PostWritable, IntWritable> countMap = new TreeMap<>((o1, o2) -> {
                if(o1.getScore().get()>o2.getScore().get()){
                    return -1;
                }
                else {
                    return 1;
                }
            });

            int count = 0;

            // Creating a new temporary final writable, to be emitted from reducer
            WeeklyWritable wW = new WeeklyWritable();
            wW.setWeek(key);


            for(WeeklyWritable val : values){     // iterating across all the weekly data, coming from mappers

                // we remove the last key-value
                // if it's size increases 5
                for(PostWritable hw : val.getPostList()){
                    count++;
                    countMap.put(hw,hw.getScore());
                    if (countMap.size() > 5) {
                        countMap.pollLastEntry();
                    }
                }
            }
            wW.addAllPostList(new ArrayList<>(countMap.keySet()));

            wW.setCount(new IntWritable(count));
            context.write(new Text("Week "+key), wW);
        }
        catch (Exception e){
            logger.error("TagCountReducer : " +e.getMessage());
        }
    }
}
