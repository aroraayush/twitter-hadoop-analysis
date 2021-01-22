package edu.usfca.cs.mr.advanced_analysis_2;

import edu.usfca.cs.mr.advanced_analysis_1.UsersWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Reducer: Input to the reducer is the output from the mapper. It receives
 * word, list<count> pairs.  Sums up individual counts per given word. Emits
 * <word, total count> pairs.
 */
public class WeeklyTweetLenReducer
extends Reducer<IntWritable, WeeklyTweetLenWritable, Text, WeeklyTweetLenWritable> {

    private static Logger logger = LoggerFactory.getLogger(WeeklyTweetLenReducer.class.getName());

    @Override
    protected void reduce(
            IntWritable key, Iterable<WeeklyTweetLenWritable> values, Context context)
    throws IOException, InterruptedException {
        try {

            // Hashmap to store names of popular users, count. Will be used to merge the data with same username pairs coming from mapper
            HashMap<Integer, TweetLenWritable> countMap1  = new HashMap<>();
            int count = 0;
            // Creating a new weekly writable to store the merged data
            WeeklyTweetLenWritable wW = new WeeklyTweetLenWritable();
            wW.setWeek(key);

            for(WeeklyTweetLenWritable val : values){  // iterating across all the users coming from mappers
                  for(TweetLenWritable uW : val.getTweetLenList()){     // iterating across all the users coming from mappers
                      int tweetLen = uW.getTweetLen().get();
                      if(!countMap1.containsKey(tweetLen)){  // Storing the user to hashmap the first time
                          countMap1.put(tweetLen,uW);
                      }
                      else {
                          // Creating a temp UsersWritable, to merge data
                          HashSet<Text> tweetUsers = new HashSet<>(countMap1.get(tweetLen).getMentionedByList());
                          tweetUsers.addAll(uW.getMentionedByList());

                          HashSet<Text> tweetDates = new HashSet<>(countMap1.get(tweetLen).getDatetimeList());
                          tweetUsers.addAll(uW.getDatetimeList());
                          TweetLenWritable temp = new TweetLenWritable(countMap1.get(tweetLen).getCount().get(),
                                  countMap1.get(tweetLen).getSentiment().get(),
                                  new ArrayList<>(tweetDates), new ArrayList<>(tweetUsers),
                                  countMap1.get(tweetLen).getTweetLen().get());

                          temp.setCount(uW.getCount()); // Merge count
                          temp.setSentiment(uW.getSentiment()); // Merge sentiment
                          countMap1.put(tweetLen,temp);
                      }
                }
                // calculate the total count
                count++;
            }

            // Extracting top 5 users for the week
            List<TweetLenWritable> arrayList = countMap1.entrySet().stream()
                    .map(Map.Entry::getValue)
                    .collect(Collectors.toList());
            DecimalFormat df = new DecimalFormat("0.00");
            for(TweetLenWritable h :arrayList){
                int count1 = h.getCount().get();
                double sent = h.getSentiment().get();
                double res = (double) sent / count1;
                res = Double.parseDouble(df.format(res));
                h.setFinalSentiment(new DoubleWritable(res));
            }
            wW.setTweetLenList(arrayList);
            wW.setCount(new IntWritable(count));
            context.write(new Text("Week "+key), wW);
        }
        catch (Exception e){
            logger.error("TagCountReducer : " +e.getMessage());
        }
    }
}
