package edu.usfca.cs.mr.sentiment_analysis;

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
 * It contains the hashtag, its count, usernames invloved, sentiments for the hashtag
 */
public class HashtagSAWritable implements Writable {

    private Text hashtag;
    private IntWritable count;
    private List<Text> usernames;
    private DoubleWritable sentiment;

    //default constructor for (de)serialization
    public HashtagSAWritable() {
        this.count = new IntWritable(0);
        this.sentiment = new DoubleWritable(0.0);
        this.hashtag = new Text("");
        this.usernames = new ArrayList<>();
    }

    public HashtagSAWritable(int count, Text hashtag, List<Text> usernames,double sentiment) {
        this.count = new IntWritable(count);
        this.hashtag = new Text(hashtag);
        this.usernames = new ArrayList<>();
        this.usernames.addAll(usernames);
        this.sentiment = new DoubleWritable(sentiment);
    }

    public void setUsernames(List<Text> usernames) {
        this.usernames.addAll(usernames);
    }

    public List<Text> getUsernames() {
        return usernames;
    }

    public Text getHashtag() {
        return hashtag;
    }

    public IntWritable getCount() {
        return count;
    }


    public DoubleWritable getSentiment() {
        return sentiment;
    }

    public void setSentiment(DoubleWritable sentiment) {
        this.sentiment = new DoubleWritable(((double) this.sentiment.get() + sentiment.get()));
    }

    public void setFinalSentiment(DoubleWritable sentiment) {
        this.sentiment = new DoubleWritable((sentiment.get()));
    }

    public void setCount(IntWritable count) {
        this.count = new IntWritable(this.count.get() + count.get());
    }

    public void write(DataOutput out) throws IOException {
        // Serializing the data to send to next machine
        this.count.write(out);
        this.hashtag.write(out);
        this.sentiment.write(out);
        out.writeInt(usernames.size());

        for(int index=0;index<usernames.size();index++){
            // Serializing every values in list to send to next machine
            usernames.get(index).write(out); //write all the value of list
        }
    }

    public void readFields(DataInput in) throws IOException {
        // deserializing
        this.count.readFields(in);
        this.hashtag.readFields(in);
        this.sentiment.readFields(in);

        int size = in.readInt(); //read size of list
        usernames = new ArrayList<>(size);

        for(int i=0;i<size;i++){ //read all the values of list
            Text user = new Text();
            user.readFields(in);
            usernames.add(user);
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("Tweet : "+ hashtag );

        sb.append("     count=" + count + "\t\t\t\t Overall Sentiment: "+sentiment+ "\t\t\t\t Users: "+usernames);

//        for(int i = 0;i<tweetList.size();i++){
//            sb.append(System.lineSeparator() + "\t\t\t" +dateTimeList.get(i) + " | Tweet : "+ tweetList.get(i));
//        }
        sb.append(System.lineSeparator());
        return sb.toString();
    }
}