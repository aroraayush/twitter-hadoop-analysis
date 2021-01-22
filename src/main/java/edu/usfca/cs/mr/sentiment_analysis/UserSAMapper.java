package edu.usfca.cs.mr.sentiment_analysis;

import edu.usfca.cs.mr.util.SentimentAnalysis;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

/**
 * Mapper: Reads 4 lines at a time, split them into words. Emit <word, 1> pairs.
 */
public class UserSAMapper
extends Mapper<LongWritable, Text, Text, UserSentimentWritable> {

    private static Logger logger = LoggerFactory.getLogger(UserSAMapper.class.getName());

    int len;
    HashSet<String> positiveWords;
    HashSet<String> negativeWords;

    @Override
    protected void setup(Context context) throws IOException {
        positiveWords = new HashSet<>();
        negativeWords = new HashSet<>();

        len = "U\thttp://twitter.com/".length();
        Configuration conf = context.getConfiguration();
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
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        try {
            String line = value.toString();
            // Check if line is invalid
            if (line == null || line.isEmpty() || line.trim().length() == 0)
                return;

            // Process a tweet only if it contains atlease one hashtag (check for '#' symbol)
            if (line.contains("#")) {
                processTweet(line, context);
            }
        }
        catch (Exception e){
            logger.error("TweetMapper : "+ e.getMessage());
        }
    }

    private void processTweet(String line, Context context) throws IOException, InterruptedException {

        // Splitting the combined 4 lines input to 4 separate lines
        String[] lines = line.split("\n");
        String tweet = null;
        Text username = null;
        String usernameStr;

        for(String l : lines){
            if(l.startsWith("W")) {   // Obtaining the tweet
                tweet = l.substring(2).trim();
            }
            else if(l.startsWith("U")) {    // Obtaining the user
                if(l.length() < len || l.substring(len).length() == 0)
                    usernameStr = "Anonymous";
                else
                    usernameStr = l.substring(len);
                username = new Text(usernameStr);
            }
        }

        if(tweet != null){
            // Storing the hashtags
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
                }
                else{
                    tokens.add(token);
                }
            }
            // obtaining the overall sentiment
            double sentiment = SentimentAnalysis.getSentimentSet(tokens, positiveWords ,negativeWords);;

            UserSentimentWritable uSW = new UserSentimentWritable(1, new ArrayList<>(tagSet) , sentiment, username);
            context.write(username, uSW);
        }
    }
}
