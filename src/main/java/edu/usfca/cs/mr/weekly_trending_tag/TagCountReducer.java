package edu.usfca.cs.mr.weekly_trending_tag;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * Reducer: It receives hastag, list<WeekWritable> pairs.  Sums up individual counts per given word. Emits
 * <word, total count> pairs.
 */
public class TagCountReducer
extends Reducer<IntWritable, WeekWritable, Text, WeekWritable> {

    private static Logger logger = LoggerFactory.getLogger(TagCountReducer.class.getName());

    @Override
    protected void reduce(
            IntWritable key, Iterable<WeekWritable> values, Context context)
    throws IOException, InterruptedException {
        try {
            // Treemap to sort according to count of tweet mentions
            // If tweet mention count is same, sort by sentiment value
            TreeMap<HashtagWritable, Text> countMap = new TreeMap<>((o1, o2) -> {
                if(o1.getCount().get()>o2.getCount().get()){
                    return -1;
                }
                else {
                    return 1;
                }
            });

            // Hashmap to store hashtag, along with its metadata
            HashMap<Text,HashtagWritable> countMap1  = new HashMap<>();

            int count = 0;

            // Creating a new temporary final writable, to be emitted from reducer
            WeekWritable wW = new WeekWritable();
            wW.setWeek(key);

            for(WeekWritable val : values){     // iterating across all the weekly data, coming from mappers

                  for(HashtagWritable hw : val.getHashTagList()){

                      Text hashtag = hw.getHashtag();
                      if(!countMap1.containsKey(hashtag)){
                          countMap1.put(hashtag,hw);
                      }
                      else {

                          countMap1.get(hashtag).setCount(hw.getCount());
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

            // Extracting top 5 weekly trending hashtags
            List<HashtagWritable> arrayList = countMap.entrySet().stream()
                    .map(Map.Entry::getKey)
                    .collect(Collectors.toList());

            // Setting all the remaining variables
            wW.setHashTagList(arrayList);
            wW.setCount(new IntWritable(count));
            context.write(new Text("Week "+key), wW);
        }
        catch (Exception e){
            logger.error("TagCountReducer : " +e.getMessage());
        }
    }
}
