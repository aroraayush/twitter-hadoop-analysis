package edu.usfca.cs.mr.advanced_analysis_2;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * It contains the user meta data (class members)
 */
public class TweetLenWritable implements Writable {

    private IntWritable count;  // count of mentions
    private IntWritable tweetLen;  // count of mentions
    private DoubleWritable sentiment;  // Overall sentiment involved of the mentioned tweets
    private List<Text> datetimeList;    // list of datetime when the popular user was mentioned
    private List<Text> mentionedByList;     // list of all users mentioned that mentioned this popular user

    //default constructor for (de)serialization
    public TweetLenWritable() {
        this.count = new IntWritable(0);
        this.tweetLen = new IntWritable(0);
        this.sentiment = new DoubleWritable(0.0);
        this.datetimeList = new ArrayList<>();
        this.mentionedByList = new ArrayList<>();
    }

    public TweetLenWritable(int count, double sentiment, List<Text> datetimeList, List<Text> mentionedByList, int tweetLen) {
        this.count = new IntWritable(count);
        this.tweetLen = new IntWritable(tweetLen);
        this.sentiment = new DoubleWritable(sentiment);

        this.datetimeList = new ArrayList<>();
        this.mentionedByList = new ArrayList<>();
        this.datetimeList.addAll(datetimeList);
        this.mentionedByList.addAll(mentionedByList);
    }

    public IntWritable getTweetLen() {
        return tweetLen;
    }

    public void setTweetLen(IntWritable tweetLen) {
        this.tweetLen = tweetLen;
    }

    public void setMentionedByList(List<Text> mentionedByList) {
        this.mentionedByList.addAll(mentionedByList);
    }

    public void setDatetimeList(List<Text> datetimeList) {
        this.datetimeList.addAll(datetimeList);
    }

    public void setSentiment(DoubleWritable sentiment) { // Adding the previous sentiment values to the current
        this.sentiment = new DoubleWritable((this.sentiment.get() + sentiment.get()));
    }

    public void setFinalSentiment(DoubleWritable sentiment) { // Adding the previous sentiment values to the current
        this.sentiment = new DoubleWritable((sentiment.get()));
    }

    // Adding the previous count value to the current
    public void setCount(IntWritable count) {
        this.count = new IntWritable(this.count.get() + count.get());
    }

    public void write(DataOutput out) throws IOException {
        // Serializing the data to send to next machine
        this.count.write(out);          // writing the count in the output stream
        this.sentiment.write(out);      // writing the sentiment in the output stream
        this.tweetLen.write(out);      // writing the custom length of tweets (150, 200 or 250)

        out.writeInt(datetimeList.size());      // writing the size of the datetime list
        for(int index=0;index<datetimeList.size();index++){
            // Serializing every values in list to send to next machine
            datetimeList.get(index).write(out); //write all the value of list
        }

        out.writeInt(mentionedByList.size());       // writing the size of the mentionedBy list
        for(int index=0;index<mentionedByList.size();index++){
            // Serializing every values in list to send to next machine
            mentionedByList.get(index).write(out); //write all the value of list
        }
    }

    public void readFields(DataInput in) throws IOException {
        // deserializing
        this.count.readFields(in);          // reading the count from the inputstream
        this.sentiment.readFields(in);      // reading the sentiment from the inputstream
        this.tweetLen.readFields(in);      // reading the tweet Length from the inputstream

        int size2 = in.readInt(); //read size of datetime list
        this.datetimeList = new ArrayList<>(size2);

        for(int i=0;i<size2;i++){ //read all the values of list
            Text dateTime = new Text();
            dateTime.readFields(in);
            datetimeList.add(dateTime);
        }

        int size3 = in.readInt();        //read size of mentionedBy list

        this.mentionedByList = new ArrayList<>(size3);

        for(int i=0;i<size3;i++){ //read all the values of list
            Text tweet = new Text();
            tweet.readFields(in);
            mentionedByList.add(tweet);
        }

    }

    public DoubleWritable getSentiment() {
        return sentiment;
    }

    public List<Text> getDatetimeList() {
        return datetimeList;
    }

    public List<Text> getMentionedByList() {
        return mentionedByList;
    }

    public IntWritable getCount() {
        return count;
    }

    @Override
    public String toString() { // Pretty printing the data
        return System.lineSeparator() + "\t\t\t Tweet Length : "+ tweetLen + " characters \t\t\t Overall Sentiment : " + sentiment +
                "% | tweeted by "+count + " people. Tweet users :" + mentionedByList  + System.lineSeparator() +
                "\t\t\t\t\t\t\t\t\t\t\t\t tweeted at : " + datetimeList + System.lineSeparator();
    }
}