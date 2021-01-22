package edu.usfca.cs.mr.weekly_trending_tag;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.IsoFields;
import java.util.*;

/**
 * Mapper: Reads line by line, split them into words. Emit <hashtag, 1> pairs.
 */
public class HashTagMapper
extends Mapper<LongWritable, Text, IntWritable, WeekWritable> {

    private static Logger logger = LoggerFactory.getLogger(HashTagMapper.class.getName());

    @Override
    protected void map(LongWritable key, Text value, Context context) {

        try {
            String line = value.toString();

            // Check if line is invalid
            if(line == null || line.isEmpty() || line.trim().length() == 0)
                return;

            // Process a tweet only if it contains atlease one hashtag (check for '#' symbol)
            if(line.contains("#")){
                Date date;
                SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
                ZonedDateTime zDt;
                String dateTimestamp;
                HashMap<Text, Integer> map = new HashMap<>();

                // Splitting the combined 4 lines input to 4 separate lines
                String[] lines = line.split("\n");
                int week = -1;

                for(String l : lines){
                    if(l.startsWith("T")){
                        dateTimestamp = l.substring(2,18);
                        date = df.parse(dateTimestamp);
                        zDt = ZonedDateTime.ofInstant(date.toInstant(), ZoneId.systemDefault());
                        week = zDt.get(IsoFields.WEEK_OF_WEEK_BASED_YEAR);
                    }
                    else if(l.startsWith("W")) {   // Obtaining the tweet
                        l = l.substring(2).trim();

                        StringTokenizer tokenizer = new StringTokenizer(l);
                        while (tokenizer.hasMoreTokens()) {
                            String token = tokenizer.nextToken().trim();
                            // Checking if the token is invalid
                            if (!token.isEmpty() && token.charAt(0) == '#') {
                                Text t = new Text(token);
                                // Storing the count to map
                                if(!map.containsKey(t)){
                                    map.put(t, 1);
                                }
                                else {
                                    map.put(t, map.get(t) + 1);
                                }
                            }
                        }
                    }
                }

                // Checking overall validity of data
                if(week!=-1 && map.keySet().size() > 0) {

                    // Creating a list of all hashtags
                    Set<HashtagWritable> hashTagSet = new HashSet<>();
                    for(Text tag: map.keySet()){
                        // Storing all hashtags for a week
                        HashtagWritable hW = new HashtagWritable(map.get(tag),tag);
                        hashTagSet.add(hW);
                    }

                    // Storing week information along with all the tweets and their count
                    WeekWritable wW = new WeekWritable(map.keySet().size(), week, new ArrayList<>(hashTagSet));
                    context.write(new IntWritable(week), wW);
                }
            }
        }
        catch (Exception e){
            logger.error("HashTagMapper : "+ e.getMessage());
        }

    }

}
