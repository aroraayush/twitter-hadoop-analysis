package edu.usfca.cs.mr.sentiment_analysis;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * It contains the user name, its count, list of hashtags used with the user,
 * sentiment for tweets where user was involved
 */
public class UserSentimentWritable implements Writable {
    
    private static Logger logger = LoggerFactory.getLogger(UserSentimentWritable.class.getName());
    private Text user;  // username
    private IntWritable count;  // count of users
    private List<Text> tagsList;    // list of all tags
    private DoubleWritable sentiment;   // overall sentiment for the user

    //default constructor for (de)serialization
    public UserSentimentWritable() {
        this.user = new Text("");
        this.count = new IntWritable(0);
        this.tagsList = new ArrayList<>();
        this.sentiment = new DoubleWritable(0.0);
    }

    public UserSentimentWritable(int count, List<Text> tagsList, double sentiment, Text user) {
        this.user = user;
        this.count = new IntWritable(count);
        this.tagsList = new ArrayList<>();
        this.tagsList.addAll(tagsList);
        this.sentiment = new DoubleWritable(sentiment);
    }

    public void setUser(Text user) {
        this.user = new Text(user);
    }

    public IntWritable getCount() {
        return count;
    }

    public DoubleWritable getSentiment() {
        return sentiment;
    }

    public void write(DataOutput out) throws IOException {
        // Serializing the data to send to next machine
        this.user.write(out); 
        this.count.write(out); 
        this.sentiment.write(out); 
        out.writeInt(tagsList.size());

        for(int index=0;index<tagsList.size();index++){
            // Serializing every values in list to send to next machine
            tagsList.get(index).write(out); //write all the value of list
        }
    }

    public void readFields(DataInput in) throws IOException {
        // deserializing
        this.user.readFields(in);
        this.count.readFields(in);
        this.sentiment.readFields(in);

        int size = in.readInt(); //read size of list
        tagsList = new ArrayList<>(size);

        for(int i=0;i<size;i++){ //read all the values of list
            Text text = new Text();
            text.readFields(in);
            tagsList.add(text);
        }
    }

    public Text getUser() {
        return user;
    }

    public List<Text> getTagsList() {
        return tagsList;
    }

    public void addAllTagsList(List<Text> tagsList) {
        this.tagsList.addAll(tagsList);
    }

    public void setSentiment(DoubleWritable sentiment) {
        this.sentiment = new DoubleWritable(this.sentiment.get()+sentiment.get());
    }

    public void setCount(IntWritable count) {
        this.count = new IntWritable(this.count.get()+count.get());
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("\t\t Total Tweets =" + count + "\t| Overall sentiment = " );
        if(sentiment.get() > 0){
            sb.append(sentiment + "% positive");
        }
        else if(sentiment.get() == 0){
            sb.append("neutral");
        }
        else {
            sb.append((-1 * sentiment.get()) + "% negative");
        }
        sb.append(System.lineSeparator());
        sb.append("\t\t\t\t\t"  + "Hashtags for the user : "+ tagsList);
        sb.append(System.lineSeparator());
        return sb.toString();
    }
}