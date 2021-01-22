package edu.usfca.cs.mr.finalProjectAnalysis1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CommentsMapper extends Mapper<LongWritable, Text, IntWritable, CommentsWritable> {
// to extract data from the comments of stack overflow, reading one line at a time
    // analyzing sentiment associated with the comments

    HashSet<Text> positiveWords;
    HashSet<Text> negativeWords;

    @Override
    protected void setup(Context context) throws IOException {
        positiveWords = new HashSet<>();
        negativeWords = new HashSet<>();

        Configuration conf = context.getConfiguration();
        String positive_word_txt_path = conf.get("positive_word_txt_path");
        String negative_word_txt_path = conf.get("negative_word_txt_path");

        Path pt= new Path(positive_word_txt_path); // Location of file in HDFS
        FileSystem fs = FileSystem.get(conf);
        BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
        String line;
        line=br.readLine();
        while (line != null){
            positiveWords.add(new Text(line.trim()));
            line=br.readLine();
        }

        Path pt2= new Path(negative_word_txt_path);//Location of file in HDFS

        BufferedReader br2=new BufferedReader(new InputStreamReader(fs.open(pt2)));
        String line2;
        line2=br2.readLine();
        while (line2 != null){
            negativeWords.add(new Text(line2.trim()));
            line2=br2.readLine();
        }
    }
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        // reading one line at a time
        String line = value.toString();
        // ignoring if the line is null, or anything unusual
        if(line == null || line.isEmpty() || line.trim().length() == 0 || !line.startsWith("  <row"))
            return;

        // breaking each line into tokens
        StringTokenizer itr = new StringTokenizer(line);
        int postId = 0;
        int score = 0;
        String date = "";
        int userId = 0;
        while (itr.hasMoreTokens()){
            String token = itr.nextToken();
            // extracting the data from each token
            if(token.startsWith("PostId")) {
                String[] reg = token.split("\"");
                postId = Integer.parseInt(reg[1]);
            }
            if(token.startsWith("Score")){
                String[] reg = token.split("\"");
                score = Integer.parseInt(reg[1]);
            }
            if(token.startsWith("CreationDate")){
                String[] reg = token.split("\"");
                date = reg[1].substring(0, 10);
            }
            if(token.startsWith("UserId")){
                String[] reg = token.split("\"");
                userId = Integer.parseInt(reg[1]);
            }
        }
        // using regex pattern to extract the comment text from the data
        Pattern p = Pattern.compile("(.)(Text=\")(.*)(\" CreationDate)");
        Matcher m = p.matcher(line);
        String commentText = "";
        if(m.find()){
            commentText = m.group(3);
        }
        // calculating the sentiment associated with the comment text
        double sentiment = calculateSentiment(new Text(commentText), positiveWords ,negativeWords);
        ArrayList<Text> commentTextList = new ArrayList<>();
        commentTextList.add(new Text(commentText));
        ArrayList<Text> dateList = new ArrayList<>();
        dateList.add(new Text(date));
        ArrayList<IntWritable> scoreList = new ArrayList<>();
        scoreList.add(new IntWritable(score));
        ArrayList<IntWritable> listUserId = new ArrayList<>();
        listUserId.add(new IntWritable(userId));

        // adding all the relevant data to custom writable object and sending to reducer
        CommentsWritable commentsWritable = new CommentsWritable(postId, commentTextList, dateList, scoreList, listUserId, sentiment, 1);
        context.write(new IntWritable(postId), commentsWritable);
    }

    // calculates sentiment between a range of 0 to 1, 0 being the most negative, 1 being the most positive.
    // using a list of positive and negative words from files saved as positive and negative words
    // giving positive or negative scores if a particular word matches in the file and is present in the tweet text
    private Double calculateSentiment(Text commentText, HashSet<Text> positiveWords , HashSet<Text> negativeWords){
        double sentiment = 0.0;
        int countPositive = 0;
        int countNegative = 0;
        StringTokenizer itr = new StringTokenizer(commentText.toString());
        HashSet<Text> tweetTextSet = new HashSet<>();
        while (itr.hasMoreTokens()){
            tweetTextSet.add(new Text(itr.nextToken()));
        }

        for(Text text : tweetTextSet){
            if(positiveWords.contains(text))
                countPositive++;
            if(negativeWords.contains(text))
                countNegative++;
        }
        if(countNegative + countPositive == 0) // if no positive or negative words found, then neutral sentiment 0.5
            sentiment = 0.5;
        else
            sentiment = (double)countPositive / (double)(countNegative + countPositive);  // fraction of positive sentiment from total sentiment

        return sentiment;
    }
}
