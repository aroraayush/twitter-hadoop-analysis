package edu.usfca.cs.mr.advanced_analysis_1;

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
public class UsersWritable implements Writable {

    private Text username;      // popular user
    private IntWritable count;  // count of mentions
    private DoubleWritable sentiment;  // Overall sentiment involved of the mentioned tweets
    private List<Text> hashTagList;     // list of hashtags, where user was mentioned
    private List<Text> datetimeList;    // list of datetime when the popular user was mentioned
    private List<Text> mentionedByList;     // list of all users mentioned that mentioned this popular user

    //default constructor for (de)serialization
    public UsersWritable() {
        this.count = new IntWritable(0);
        this.sentiment = new DoubleWritable(0.0);
        this.hashTagList = new ArrayList<>();
        this.datetimeList = new ArrayList<>();
        this.mentionedByList = new ArrayList<>();
        this.username = new Text("");
    }

    public UsersWritable(int count, double sentiment, List<Text> hashTagList, List<Text> datetimeList, List<Text> mentionedByList, Text username) {
        this.count = new IntWritable(count);
        this.sentiment = new DoubleWritable(sentiment);

        this.hashTagList = new ArrayList<>();
        this.datetimeList = new ArrayList<>();
        this.mentionedByList = new ArrayList<>();

        this.hashTagList.addAll(hashTagList);
        this.datetimeList.addAll(datetimeList);
        this.mentionedByList.addAll(mentionedByList);

        this.username = new Text(username);
    }

    public void setHashTagList(List<Text> hashTagList) {
        this.hashTagList.addAll(hashTagList);
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
        this.username.write(out);       // writing the username in the output stream
        out.writeInt(hashTagList.size());          // writing the size of the hashtags list

        for(int index=0;index<hashTagList.size();index++){
            // Serializing every values in list to send to next machine
            hashTagList.get(index).write(out); //write all the value of list
        }

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
        this.username.readFields(in);       // reading the username from the inputstream

        int size = in.readInt();            //read size of  hashTaglist
        this.hashTagList = new ArrayList<>(size);

        for(int i=0;i<size;i++){ //read all the values of list
            Text tag = new Text();
            tag.readFields(in);
            hashTagList.add(tag);
        }

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

    public List<Text> getHashTagList() {
        return hashTagList;
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


    public Text getUsername() {
        return username;
    }

    public void setUsername(Text username) {
        this.username = username;
    }

    @Override
    public String toString() { // Pretty printing the data
        return System.lineSeparator() + "\t\t\tUser : " + username +
                ", mentioned by " + count +
                " people. " + mentionedByList  + System.lineSeparator() +
                "\t\t\t\t\t\t Overall Sentiment : " + sentiment +
                "\t\t\t\t\t\t Hashtags used : " + hashTagList + System.lineSeparator() +
                "\t\t\t\t\t\t mentioned at : " + datetimeList ;
    }
}