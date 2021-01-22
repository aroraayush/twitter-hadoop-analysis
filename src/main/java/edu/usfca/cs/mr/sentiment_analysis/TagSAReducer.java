package edu.usfca.cs.mr.sentiment_analysis;

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
 * week, list<WeekSAWritable> pairs. Each WeekSAWritable contains the count for the week.
 * Reducer Sums up individual counts per given week. Emits
 * <week, total count + metadata> pairs.
 */
public class TagSAReducer
extends Reducer<IntWritable, WeekSAWritable, Text, WeekSAWritable> {

    private static Logger logger = LoggerFactory.getLogger(TagSAReducer.class.getName());

    @Override
    protected void reduce(
            IntWritable key, Iterable<WeekSAWritable> values, Context context)
    throws IOException, InterruptedException {
        try {

            // Treemap to sort according to count of tweet
            TreeMap<HashtagSAWritable, Text> countMap = new TreeMap<>((o1, o2) -> {
                if(o1.getCount().get()>o2.getCount().get()){
                    return -1;
                }
                else {
                    return 1;
                }
            });

            // Hashmap to store hashtags, count. Will be used to merge the data with same hashtag pairs coming from mapper
            HashMap<Text,HashtagSAWritable> countMap1  = new HashMap<>();

            int count = 0;

            // Creating a new temporary final writable, to be emitted from reducer
            WeekSAWritable wW = new WeekSAWritable();
            wW.setWeek(key);

            for(WeekSAWritable val : values){    // iterating across all the weekly data, coming from mappers
                  for(HashtagSAWritable hw : val.getHashTagList()){
                      Text hashtag = hw.getHashtag();
                      if(!countMap1.containsKey(hashtag)){
                          countMap1.put(hashtag,hw);
                      }
                      else {
                          // Merging all the data
                          countMap1.get(hashtag).setCount(hw.getCount());
                          countMap1.get(hashtag).setUsernames(hw.getUsernames());
                          countMap1.get(hashtag).setSentiment(hw.getSentiment());
                          countMap1.put(hashtag,hw);
                      }
                }
                // calculate the total count
                count++;
            }

            // we remove the last key-value
            // if it's size increases 5
            for(Text tag: countMap1.keySet()){
                countMap.put(countMap1.get(tag),tag);
                if (countMap.size() > 5) {
                    countMap.pollLastEntry();
                }
            }

            // Extracting top 5 hashtags
            List<HashtagSAWritable> arrayList = countMap.entrySet().stream()
                    .map(Map.Entry::getKey)
                    .collect(Collectors.toList());
            wW.setCount(new IntWritable(count));
            DecimalFormat df = new DecimalFormat("0.00");
            for(HashtagSAWritable h :arrayList){
                int count1 = h.getCount().get();
                double sent = h.getSentiment().get();
                double res = (double) sent / count1;
                res = Double.parseDouble(df.format(res));
                h.setFinalSentiment(new DoubleWritable(res));

            }
            wW.setHashTagList(arrayList);
            context.write(new Text("Week "+key), wW);
        }
        catch (Exception e){
            logger.error("TagCountReducer : " +e.getMessage());
        }
    }
}
