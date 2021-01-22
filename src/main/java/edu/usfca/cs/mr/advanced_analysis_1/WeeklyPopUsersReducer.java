package edu.usfca.cs.mr.advanced_analysis_1;

import edu.usfca.cs.mr.sentiment_analysis.HashtagSAWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * Reducer: Input to the reducer is the output from the mapper. It receives
 * word, list<count> pairs.  Sums up individual counts per given word. Emits
 * <word, total count> pairs.
 */
public class WeeklyPopUsersReducer
extends Reducer<IntWritable, WeeklyUsersWritable, Text, WeeklyUsersWritable> {

    private static Logger logger = LoggerFactory.getLogger(WeeklyPopUsersReducer.class.getName());

    @Override
    protected void reduce(
            IntWritable key, Iterable<WeeklyUsersWritable> values, Context context)
    throws IOException, InterruptedException {
        try {

            // Treemap to sort according to count of tweet
            // If tweet mention count is same, sort by sentiment value
            TreeMap<UsersWritable, Text> countMap = new TreeMap<>((o1, o2) -> {
                if(o1.getCount().get()==o2.getCount().get()){
                    int sent =(int) (o1.getSentiment().get() - o2.getSentiment().get());
                    if (sent == 0)
                        return 1;
                    return sent;
                }
                if(o1.getCount().get()>o2.getCount().get()){
                    return -1;
                }
                else {
                    return 1;
                }
            });

            // Hashmap to store names of popular users, count. Will be used to merge the data with same username pairs coming from mapper
            HashMap<Text,UsersWritable> countMap1  = new HashMap<>();
            int count = 0;
            // Creating a new weekly writable to store the merged data
            WeeklyUsersWritable wW = new WeeklyUsersWritable();
            wW.setWeek(key);

            for(WeeklyUsersWritable val : values){  // iterating across all the users coming from mappers
                  for(UsersWritable uW : val.getusersList()){     // iterating across all the users coming from mappers
                      Text username = uW.getUsername();
                      if(!countMap1.containsKey(username)){  // Storing the user to hashmap the first time
                          countMap1.put(username,uW);
                      }
                      else {
                          // Creating a temp UsersWritable, to merge data
                          UsersWritable temp = new UsersWritable(countMap1.get(username).getCount().get(),countMap1.get(username).getSentiment().get(),countMap1.get(username).getHashTagList(),countMap1.get(username).getDatetimeList(),countMap1.get(username).getMentionedByList(),countMap1.get(username).getUsername());
                          temp.setCount(uW.getCount()); // Merge count
                          temp.setSentiment(uW.getSentiment()); // Merge sentiment

                          temp.setHashTagList(uW.getHashTagList());     // Merge hashtags
                          temp.setDatetimeList(uW.getDatetimeList());   // Merge date time list
                          temp.setMentionedByList(uW.getMentionedByList());  // Merge mentioned by user's list
                          countMap1.put(username,temp);
                      }
                }
                // calculate the total count
                count++;
            }

            // we remove the last key-value
            // if it's size increases 5
            for(Text tag: countMap1.keySet()){      // Removing users other than top 5
                countMap.put(countMap1.get(tag),tag);
                if (countMap.size() > 5) {
                    countMap.pollLastEntry();
                }
            }
            // Extracting top 5 users for the week
            List<UsersWritable> arrayList = countMap.entrySet().stream()
                    .map(Map.Entry::getKey)
                    .collect(Collectors.toList());

            DecimalFormat df = new DecimalFormat("0.00");
            for(UsersWritable h :arrayList){
                int count1 = h.getCount().get();
                double sent = h.getSentiment().get();
                double res = (double) sent / count1;
                res = Double.parseDouble(df.format(res));
                h.setFinalSentiment(new DoubleWritable(res));
            }
            wW.setUsersList(arrayList);
            wW.setCount(new IntWritable(count));
            context.write(new Text("Week "+key), wW);
        }
        catch (Exception e){
            logger.error("TagCountReducer : " +e.getMessage());
        }
    }
}
