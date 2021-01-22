package edu.usfca.cs.mr.top_users;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Mapper: Reads 4 lines at a time, split them into words. Emit <word, 1> pairs.
 */
public class TweetMapper
extends Mapper<LongWritable, Text, Text, TweetWritable> {

    private static Logger logger = LoggerFactory.getLogger(TweetMapper.class.getName());
    String twitterStr = "U\thttp://twitter.com/";
    int len = twitterStr.length();

    @Override
    protected void setup(Context context) { }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        try {
            String line = value.toString();
            // Check if line is invalid
            if(line == null || line.isEmpty() || line.trim().length() == 0)
                return;

            // Splitting the combined 4 lines input to 4 separate lines
            String[] lines = line.split("\n");
            List<Text> dateTimeList = new ArrayList<>();
            List<Text> tweetList = new ArrayList<>();
            Text username = null;

            String usernameStr;

            for(String l : lines){
                if(l.startsWith("T")){
                    dateTimeList.add(new Text(l.substring(2,18)));
                }
                else if(l.startsWith("W")) {   // Obtaining the tweet
                    tweetList.add(new Text(l.substring(2)));
                }
                else if(l.startsWith("U")) {    // Obtaining the user
                    if(l.length() < len || l.substring(len).length() == 0)
                        usernameStr = "Anonymous";
                    else
                        usernameStr = l.substring(len);
                    username = new Text(usernameStr);
                }
            }

            // Checking validity for required data
            if(username != null && dateTimeList.size()>0 && tweetList.size()>0) {

                // Storing all tweets for a user, his/her username, along with the date time of when the user posted
                TweetWritable tw = new TweetWritable(1, tweetList, dateTimeList, username);

                context.write(username, tw);
                }
            }
        catch (Exception e){
            logger.error("TweetMapper : "+ e.getMessage());
        }
    }
}
