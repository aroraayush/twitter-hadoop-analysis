package edu.usfca.cs.mr.top_users;

import org.apache.hadoop.io.*;
import org.apache.hadoop.io.IntWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Custom writable for the user along with the tweet and datetime when s/he tweeted.
 */
public class TweetWritable implements Writable {

    private static Logger logger = LoggerFactory.getLogger(TweetWritable.class.getName());

    // Basic paramters for the writable
    private Text user;  // username
    private IntWritable count; // total count of tweets by the user
    private List<Text> tweetList;   // list of all tweets by the user
    private List<Text> dateTimeList;    // list of data time when user posted tweet

    //default constructor for (de)serialization
    public TweetWritable() {
        count = new IntWritable(0);
        this.tweetList = new ArrayList<>();
        this.dateTimeList = new ArrayList<>();
        this.user = new Text("");
    }

    public TweetWritable(int count, List<Text> tweetList, List<Text> dateTimeList, Text user) {
        this.count = new IntWritable(count);
        this.tweetList = new ArrayList<>();
        this.dateTimeList = new ArrayList<>();
        this.tweetList.addAll(tweetList);
        this.dateTimeList.addAll(dateTimeList);
        this.user = user;
    }

    public void setUser(Text user) {
        this.user = new Text(user);
    }

    public IntWritable getCount() {
        return count;
    }

    public void write(DataOutput out) throws IOException {

        // Serializing the data to send to next machine
        this.count.write(out);
        this.user.write(out);
        out.writeInt(tweetList.size());

        for(int index=0;index<tweetList.size();index++){
            // Serializing every values in list to send to next machine
            tweetList.get(index).write(out); //write all the value of list
        }

        for(int index = 0; index< dateTimeList.size(); index++){
            dateTimeList.get(index).write(out); //write all the value of list
        }
    }

    public void readFields(DataInput in) throws IOException {
        // deserializing
        this.count.readFields(in);
        user.readFields(in);

        //read size of list
        int size = in.readInt();
        tweetList = new ArrayList<>(size);
        dateTimeList = new ArrayList<>(size);

        //read all the values of list
        for(int i=0;i<size;i++){
            Text text = new Text();
            text.readFields(in);
            tweetList.add(text);
        }

        //read all the values of list
        for(int i=0;i<size;i++){
            Text text = new Text();
            text.readFields(in);
            dateTimeList.add(text);
        }
    }

    public List<Text> getDateTimeList() {
        return this.dateTimeList;
    }

    public Text getUser() {
        return user;
    }

    public List<Text> getTweetList() {
        return tweetList;
    }

    public void addAllDateTimeList(List<Text> dateTimeList) {
        this.dateTimeList.addAll(dateTimeList);
    }

    public void addAllTweetList(List<Text> tweetList) {
        this.tweetList.addAll(tweetList);
    }

    public void setCount(IntWritable count) {
        this.count = new IntWritable(this.count.get()+count.get());
    }

    public void addTotalCount(IntWritable count) {
        this.count = new IntWritable(this.count.get()+count.get());
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("count = " + count);
        for(int i = 0;i<tweetList.size();i++){
            sb.append(System.lineSeparator() + "\t\t\t" +dateTimeList.get(i) + " | Tweet : "+ tweetList.get(i));
        }
        sb.append(System.lineSeparator());
        return sb.toString();
    }
}