package edu.usfca.cs.mr.sentiment_analysis;

import edu.usfca.cs.mr.util.SentimentAnalysis;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.IsoFields;
import java.util.*;

/**
 * Mapper: Reads 4 lines at a time, split them into words. Emit <week, week metadata> pairs.
 * The week metadata includes the total count.
 *
 */
public class TagSAMapper
extends Mapper<LongWritable, Text, IntWritable, WeekSAWritable> {

    private static Logger logger = LoggerFactory.getLogger(TagSAMapper.class.getName());

    Date date;
    SimpleDateFormat df;
    ZonedDateTime zDt;
    String dateTimestamp;
    int len;
    HashSet<String> positiveWords;
    HashSet<String> negativeWords;

    @Override
    protected void setup(Context context) throws IOException {
        positiveWords = new HashSet<>();
        negativeWords = new HashSet<>();

        Configuration conf = context.getConfiguration();
        String positive_word_txt_path = conf.get("positive_word_txt_path");
        String negative_word_txt_path = conf.get("negative_word_txt_path");

        len = "U\thttp://twitter.com/".length();
        df = new SimpleDateFormat("yyyy-MM-dd");

        Path pt= new Path(positive_word_txt_path); // Location of file in HDFS
        FileSystem fs = FileSystem.get(conf);
        BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
        String line;
        line=br.readLine();
        while (line != null){
            positiveWords.add(line.trim());
            line=br.readLine();
        }

        Path pt2= new Path(negative_word_txt_path);//Location of file in HDFS

        BufferedReader br2=new BufferedReader(new InputStreamReader(fs.open(pt2)));
        String line2;
        line2=br2.readLine();
        while (line2 != null){
            negativeWords.add(line2.trim());
            line2=br2.readLine();
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) {

        try {
            String line = value.toString();
            // Check if line is invalid
            if(line == null || line.isEmpty() || line.trim().length() == 0)
                return;
            // Process a tweet only if it contains atlease one hashtag (check for '#' symbol)
            if(line.contains("#")){
                processTweet(line,context);
            }
        }
        catch (Exception e){
            logger.error("TagSAMapper : "+ e.getMessage());
        }

    }

    private void processTweet(String line, Context context) throws ParseException, IOException, InterruptedException {
        // Splitting the combined 4 lines input to 4 separate lines
        String[] lines = line.split("\n");
        int week = -1;
        String tweet = null;
        Text username = null;
        String usernameStr;

        for(String l : lines){
            // Obtaining the week and datetime
            if (l.startsWith("T")) {
                dateTimestamp = l.substring(2, 18);
                date = df.parse(dateTimestamp);
                zDt = ZonedDateTime.ofInstant(date.toInstant(), ZoneId.systemDefault());
                week = zDt.get(IsoFields.WEEK_OF_WEEK_BASED_YEAR);
            }
            else if (l.startsWith("W")) {  // Obtaining the tweet
                tweet = l.substring(2).trim();
            } else if (l.startsWith("U")) { // Obtaining the user
                if (l.length() < len || l.substring(len).length() == 0)
                    usernameStr = "Anonymous";
                else
                    usernameStr = l.substring(len);
                username = new Text(usernameStr);
            }
        }

        // Store the frequency for each hashtag
        HashMap<Text, Integer> hashTagMap = new HashMap<>();
        // Storing all the unique hashtags
        Set<Text> tagSet = new HashSet<>();
        // Storing the string tokens for sentiment analysis
        Set<String> tokens = new HashSet<>();
        StringTokenizer tokenizer = new StringTokenizer(tweet);

        while (tokenizer.hasMoreTokens()) {
            String token = tokenizer.nextToken().trim();
            Text t = new Text(token);
            // Checking if the token is invalid
            if (!token.isEmpty() && token.charAt(0) == '#') {
                tagSet.add(t);
                tokens.add(token);
                // Storing the count to map
                if(!hashTagMap.containsKey(t)){
                    hashTagMap.put(t, 1);
                }
                else {
                    hashTagMap.put(t, hashTagMap.get(t) + 1);
                }
            }
            else{
                tokens.add(token);
            }
        }

        // Checking overall validity of data
        if(week!=-1 && hashTagMap.keySet().size() > 0) {
            // obtaining the overall sentiment
            double sentiment = SentimentAnalysis.getSentimentSet(tokens, positiveWords ,negativeWords);

            List<Text> usernames = new ArrayList<>();
            usernames.add(username);

            for(Text tag: hashTagMap.keySet()){
                // Creating a new writable to store hashtag, its total usage count in tweet, username that used the hashtag, sentiment involved
                HashtagSAWritable tW = new HashtagSAWritable(hashTagMap.get(tag),tag,usernames,sentiment);

                List<HashtagSAWritable> hashTagList = new ArrayList<>();
                hashTagList.add(tW);

                // Creating a new writable to weekly data containing week number hashtag meta data, count 1 to emit <week,1>
                WeekSAWritable wW = new WeekSAWritable(1, week,hashTagList);
                context.write(new IntWritable(week), wW);
            }
//            TagSAWritable wW = new TagSAWritable(map.keySet().size(), hashTagList, username, sentiment);
//            context.write(new IntWritable(week), wW);
        }

    }
}
