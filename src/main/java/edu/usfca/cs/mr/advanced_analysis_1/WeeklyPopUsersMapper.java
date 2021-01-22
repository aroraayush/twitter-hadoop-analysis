package edu.usfca.cs.mr.advanced_analysis_1;

import edu.usfca.cs.mr.util.SentimentAnalysis;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 * Mapper: Reads 4 lines at a time, split them into words. Emit <week, week metadeta> pairs.
 * This creates the WeeklyUsersWritable and UsersWritable objects and emits them for shuffling
 * The UsersWritable stores the meta-data of popular users
 * The WeeklyUsersWritable generates all the UsersWritables w.r.t a week
 */
public class WeeklyPopUsersMapper
extends Mapper<LongWritable, Text, IntWritable, WeeklyUsersWritable> {

    private static Logger logger = LoggerFactory.getLogger(WeeklyPopUsersMapper.class.getName());

    // Setting up the variables that will be reused throughout the map operations
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
        df = new SimpleDateFormat("yyyy-MM-dd");
        len = "U\thttp://twitter.com/".length();

        String positive_word_txt_path = conf.get("positive_word_txt_path");
        String negative_word_txt_path = conf.get("negative_word_txt_path");

        Path pt= new Path(positive_word_txt_path);//Location of file in HDFS

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
            if(line.contains("@")){     // Process a tweet only if it contains '@' symbol
                processTweet(line,context);
            }
        }
        catch (Exception e){
            e.printStackTrace();
//            logger.error("WeeklyPopUsersMapper : "+ e.getMessage());
        }

    }

    /**
     * Processes the 4 lines tweet information. Extracts the popular users from tweets, gets sentiment value
     * from the tweet
     */
    private void processTweet(String line, Context context) throws ParseException, IOException, InterruptedException {

        // Splitting the combined 4 lines input to 4 separate lines
        String[] lines = line.split("\n");
        int week = -1;
        String tweet = null;
        String usernameStr;
        Text username = new Text("");

        for(String l : lines){

            if (l.startsWith("T")) {    // Obtaining the week and datetime

                dateTimestamp = l.substring(2, 18);
                date = df.parse(dateTimestamp);
                zDt = ZonedDateTime.ofInstant(date.toInstant(), ZoneId.systemDefault());
                week = zDt.get(IsoFields.WEEK_OF_WEEK_BASED_YEAR);
            }
            else if (l.startsWith("W")) {  // Obtaining the tweet

                tweet = l.substring(2).trim();
            }
            else if (l.startsWith("U")) {   // Obtaining the user
                if (l.length() < len || l.substring(len).length() == 0)
                    usernameStr = "Anonymous";
                else
                    usernameStr = l.substring(len);
                username = new Text(usernameStr);
            }
        }

        // Storing the hashtags
        HashMap<Text, Integer> popularUsersMap = new HashMap<>();
        // Storing the hashtags
        HashSet<Text> tagSet = new HashSet<>();
        // Storing the users who tweeted & mentioned these people
        HashSet<Text> mentionedBySet = new HashSet<>();
        // Storing the dates
        HashSet<Text> dateSet = new HashSet<>();
        // Storing the string tokens for sentiment analysis
        Set<String> tokens = new HashSet<>();
        
        mentionedBySet.add(username);
        dateSet.add(new Text(dateTimestamp));

        // cleaning the tweet message. Removing the extra keywords
        tweet = SentimentAnalysis.cleanBody(tweet);
        StringTokenizer tokenizer = new StringTokenizer(tweet);

        while (tokenizer.hasMoreTokens()) {
            // String token
            String token = tokenizer.nextToken().trim();
            Text t = new Text(token);

            // Checking if the token is invalid
            if (!token.isEmpty()) {
                // Checking if the token is the name of the popular user
                if (token.charAt(0) == '@') {
                    if(token.length()>1){
                        // Adding the popular users, count (1) to map
                        if (!popularUsersMap.containsKey(t)) {
                            popularUsersMap.put(t, 1);
                        } else {    // Adding the popular users, new count to map
                            popularUsersMap.put(t, popularUsersMap.get(t) + 1);
                        }
                    }
                }
                // Storing the hashtags
                else if (token.charAt(0) == '#') {
                    tagSet.add(t);
                    // Adding hashtag to sentiment analysis token as well, just
                    // in case a hashtag representation an emotion. For e.g.: #happy
                    tokens.add(token.substring(1).toLowerCase());
                } else {
                    tokens.add(token.toLowerCase());
                }
            }
        }

        // Checking the validity of variables
        if(week!=-1 && popularUsersMap.keySet().size() > 0) {

            // obtaining the overall sentiment
            double sentiment = SentimentAnalysis.getSentimentSet(tokens, positiveWords, negativeWords);

            for(Text user: popularUsersMap.keySet()){ // Emitting for each popular user
                // Storing the overall sentiment for the user, hashtags, dates when popular user were mentioned, users who tagged/mentioned the popular users, user
                UsersWritable tW = new UsersWritable(popularUsersMap.get(user),sentiment, new ArrayList<>(tagSet),new ArrayList<>(dateSet), new ArrayList<>(mentionedBySet),user);
                List<UsersWritable> userList = new ArrayList<>();
                userList.add(tW);
                // Storing the data for the overall week and emitting
                WeeklyUsersWritable wW = new WeeklyUsersWritable(1, week,userList);
                context.write(new IntWritable(week), wW);
            }
        }

    }
}
