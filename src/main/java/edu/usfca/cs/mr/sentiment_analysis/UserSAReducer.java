package edu.usfca.cs.mr.sentiment_analysis;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.TreeMap;

/**
 * Reducer: Input to the reducer is the output from the mapper. It receives
 * word, list<count> pairs.  Sums up individual counts per given word. Emits
 * <word, total count> pairs.
 */
public class UserSAReducer
extends Reducer<Text, UserSentimentWritable, Text, UserSentimentWritable> {

    private static Logger logger = LoggerFactory.getLogger(UserSAReducer.class.getName());

    // Ref : https://dzenanhamzic.com/2016/09/21/java-mapreduce-for-top-n-twitter-hashtags/

    private TreeMap<UserSentimentWritable, Text> countMap;

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
            Text key, Iterable<UserSentimentWritable> values, Context context)
    throws IOException, InterruptedException {

        try {
            int count = 0;

            // Storing all the unique hashtags from all the iterables
            HashSet<Text> tagsSet = new HashSet<>();

            double sentiment = 0.0;

            // Creating a new temporary final writable, to be emitted from reducer
            UserSentimentWritable tw = new UserSentimentWritable();
            tw.setUser(key);

            for(UserSentimentWritable val : values){    // iterating across all the user, coming from mappers
                // calculate the total count
                count += val.getCount().get();
                sentiment += val.getSentiment().get();
                tagsSet.addAll(val.getTagsList());
            }
            tw.addAllTagsList(new ArrayList<>(tagsSet));
            tw.setCount(new IntWritable(count));
            sentiment = (double) ((double)sentiment / count);
            DecimalFormat df = new DecimalFormat("0.00");
            tw.setSentiment(new DoubleWritable(Double.parseDouble(df.format(sentiment))));

            countMap.put(tw,key);

            // we remove the last key-value
            // if it's size increases 5
            if (countMap.size() > 5) {
                countMap.pollLastEntry();
            }
        }
        catch (Exception e){
            logger.error("Reducer Error : " +e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * this method runs after the reducer has seen all the values.
     * it is used to output top 5 users in file.
     */
    @Override
    protected void cleanup(Context context) throws IOException,
            InterruptedException {

        for (UserSentimentWritable key : countMap.keySet()) {
            context.write(key.getUser(), key);
        }
    }
}
