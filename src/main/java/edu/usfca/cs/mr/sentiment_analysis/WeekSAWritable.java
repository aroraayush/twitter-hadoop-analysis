package edu.usfca.cs.mr.sentiment_analysis;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * It contains the week number, total hashtag count in the week
 * and all the hashtags data for the week.
 */
public class WeekSAWritable implements Writable {

    private IntWritable week;
    private IntWritable count;
    private List<HashtagSAWritable> hashTagList;
    
    //default constructor for (de)serialization
    public WeekSAWritable() {
        this.count = new IntWritable(0);
        this.week = new IntWritable(0);
        this.hashTagList = new ArrayList<>();
    }

    public WeekSAWritable(int count, int week, List<HashtagSAWritable> hashTagList) {
        this.count = new IntWritable(count);
        this.week = new IntWritable(week);
        if(this.hashTagList == null){
            this.hashTagList = new ArrayList<>();
        }
        this.hashTagList.addAll(hashTagList);
    }

    public void write(DataOutput out) throws IOException {
        // Serializing the data to send to next machine
        this.count.write(out);
        this.week.write(out);
        out.writeInt(hashTagList.size());

        for(int index=0;index<hashTagList.size();index++){
            // Serializing every values in list to send to next machine
            hashTagList.get(index).write(out); //write all the value of list
        }
    }

    public void readFields(DataInput in) throws IOException {
        // deserializing
        count.readFields(in);
        week.readFields(in);

        int size = in.readInt(); //read size of list
        hashTagList = new ArrayList<>(size);

        for(int i=0;i<size;i++){ //read all the values of list
            HashtagSAWritable hw = new HashtagSAWritable();
            hw.readFields(in);
            hashTagList.add(hw);
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

    public List<HashtagSAWritable> getHashTagList() {
        return hashTagList;
    }

    public void setHashTagList(List<HashtagSAWritable> hashTagList) {
        this.hashTagList = hashTagList;
    }

    public void addAllHashTagList(List<HashtagSAWritable> hashTagList) {
        this.hashTagList.addAll(hashTagList);
    }

    public void addTotalCount(IntWritable count) {
        this.count = new IntWritable(this.count.get()+count.get());
    }

    @Override
    public String toString() {
        return "\t\t Total hashtags " + count + System.lineSeparator() +
                 hashTagList + System.lineSeparator();
    }
}