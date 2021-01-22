package edu.usfca.cs.mr.stack_overflow_highest_rep_users;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.TreeMap;

/**
 * Reducer: Input to the reducer is the output from the mapper. It receives
 * word, list<count> pairs.  Sums up individual counts per given word. Emits
 * <word, total count> pairs.
 */
public class HightestRepUserReducer
extends Reducer<Text, LocationWritable, Text, LocationWritable> {

    private static Logger logger = LoggerFactory.getLogger(HightestRepUserReducer.class.getName());

    @Override
    protected void reduce(
            Text key, Iterable<LocationWritable> values, Context context)
    throws IOException, InterruptedException {
        try {

            // Treemap to sort according to reputaion count of users
            // If reputation count is same, sort by upvotes
            TreeMap<RepUserWritable, Text> countMap1 = new TreeMap<>((o1, o2) -> {
                if(o1.getReputation().get() ==o2.getReputation().get()){
                    int upvotes = o1.getUpvotes().get() - o2.getUpvotes().get();
                    if(upvotes == 0)
                        return 1;
                    return upvotes;
                }
                else if(o1.getReputation().get()>o2.getReputation().get()){
                    return -1;
                }
                else {
                    return 1;
                }
            });

            int count = 0;

            // Creating a new temporary final writable, to be emitted from reducer
            LocationWritable wW = new LocationWritable();
            wW.setLocation(key);

            for(LocationWritable val : values){     // iterating across all the location data, coming from mappers
                  for(RepUserWritable hw : val.getUserList()){
                    count++;
                      countMap1.put(hw, hw.getDisplayName());
                      if (countMap1.size() > 5) {
                          countMap1.pollLastEntry();
                      }
                }
                // calculate the total count
            }

            ArrayList<RepUserWritable> list = new ArrayList<>(countMap1.keySet());
            wW.setUserList(list);
            wW.setCount(new IntWritable(count));
            context.write(key, wW);
        }
        catch (Exception e){
            logger.error("TagCountReducer : " +e.getMessage());
        }
    }
}
