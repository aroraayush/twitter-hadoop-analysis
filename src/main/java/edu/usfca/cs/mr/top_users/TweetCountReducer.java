package edu.usfca.cs.mr.top_users;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * Reducer: It receives user, list<TweetWritable> pairs.
 * Sums up individual counts per given user. Merges all the data for all the users.
 * Emits <word, total count> pairs.
 */
public class TweetCountReducer
extends Reducer<Text, TweetWritable, Text, TweetWritable> {

    private static Logger logger = LoggerFactory.getLogger(TweetCountReducer.class.getName());

    // A treemap that sorts users w.r.t to their tweet counts
    private TreeMap<TweetWritable, Text> countMap;

    @Override
    public void setup(Context context){
        countMap = new TreeMap<>((o1, o2) -> {
            if(o1.getCount().get()>o2.getCount().get()){
                return -1;
            }
            else {
                return 1;
            }
        });
    }

    @Override
    protected void reduce(
            Text key, Iterable<TweetWritable> values, Context context)
    throws IOException, InterruptedException {

        try {
            int count = 0;
            TweetWritable tw = new TweetWritable();
            tw.setUser(key);


            // Merging all the data for all users.
            for(TweetWritable val : values){      // iterating across all the users coming from mappers
                // calculate the total count
                count++;
                tw.addAllTweetList(val.getTweetList());
                tw.addAllDateTimeList(val.getDateTimeList());
            }

            tw.setCount(new IntWritable(count));
            countMap.put(tw,key);

            // we remove the last key-value
            // if it's size increases 5
            if (countMap.size() > 5) {
                countMap.pollLastEntry();
            }
        }
        catch (Exception e){
            logger.error("Reducer Error : " +e.getMessage());
        }
    }

    /**
     * this method runs after the reducer has seen all the values.
     * it is used to output top 5 users in file.
     */
    @Override
    protected void cleanup(Context context) throws IOException,
            InterruptedException {

        // Emitting the final user count, aglong with the relavant data
        for (TweetWritable key : countMap.keySet()) {
            context.write(key.getUser(), key);
        }
    }
}
