package edu.usfca.cs.mr.weekly_trending_tag;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Container for the  custom Hashtag writable, it contains the hash tag and its count
 */
public class HashtagWritable implements Writable {

    private Text hashtag;
    private IntWritable count;

    //default constructor for (de)serialization
    public HashtagWritable() {
        count = new IntWritable(0);
        this.hashtag = new Text("");
    }

    public HashtagWritable(int count, Text hashtag) {
        this.count = new IntWritable(count);
        this.hashtag = new Text(hashtag);
    }

    public Text getHashtag() {
        return hashtag;
    }

    public IntWritable getCount() {
        return count;
    }

    public void setCount(IntWritable count) {
        this.count = new IntWritable(this.count.get() + count.get());
    }

    public void write(DataOutput out) throws IOException {
        // Serializing the data to send to next machine
        this.count.write(out);
        this.hashtag.write(out);
    }

    public void readFields(DataInput in) throws IOException {
        // deserializing
        this.count.readFields(in);
        this.hashtag.readFields(in);
    }

    @Override
    public String toString() {
        return System.lineSeparator() + "\t\t\t" + hashtag +
                ", count=" + count ;
    }
}