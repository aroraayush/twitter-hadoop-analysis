package edu.usfca.cs.mr.advanced_analysis_2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Custom writable to store data for the week
 * Contains week number, total count of user mentions in the week
 * Popular user's for the week
 */
public class WeeklyTweetLenWritable implements Writable {

    private IntWritable week;   // week number
    private IntWritable count;  //  total count of popular users in the week
    private List<TweetLenWritable> tweetLenList;  // List of popular user's

    //default constructor for (de)serialization
    public WeeklyTweetLenWritable() {
        this.count = new IntWritable(0);
        this.week = new IntWritable(0);
        this.tweetLenList = new ArrayList<>();
    }

    // Constructor for storing data for mapper
    public WeeklyTweetLenWritable(int count, int week, List<TweetLenWritable> usersList) {
        this.count = new IntWritable(count);
        this.week = new IntWritable(week);
        this.tweetLenList = new ArrayList<>();
        this.tweetLenList.addAll(usersList);
    }

    // Serializing the data to send to next machine
    public void write(DataOutput out) throws IOException {
        this.count.write(out);      // Serializing the count
        this.week.write(out);       // Serializing the week number
        out.writeInt(tweetLenList.size());     // Serializing the length of users

        for(int index = 0; index< tweetLenList.size(); index++){
            // Serializing every values in list to send to next machine
            tweetLenList.get(index).write(out); //write all the value of list
        }
    }

    public void readFields(DataInput in) throws IOException {
        // deserializing
        this.count.readFields(in);
        this.week.readFields(in);

        int size = in.readInt(); //read size of users list
        tweetLenList = new ArrayList<>(size);

        for(int i = 0 ; i < size ; i++){    //read all the values of users list
            TweetLenWritable hw = new TweetLenWritable();
            hw.readFields(in);
            tweetLenList.add(hw);
        }
    }

    public IntWritable getWeek() {
        return week;
    }

    public void setWeek(IntWritable week) {
        this.week = week;
    }

    public IntWritable getCount() {
        return count;
    }

    public void setCount(IntWritable count) {
        this.count = new IntWritable(this.count.get()+count.get());
    }

    public List<TweetLenWritable> getTweetLenList() {
        return tweetLenList;
    }

    public void setTweetLenList(List<TweetLenWritable> tweetLenList) {
        this.tweetLenList.addAll(tweetLenList);
    }

    @Override
    public String toString() {
        return "\t  Total tweets count " + count + System.lineSeparator() +
                tweetLenList;
    }
}