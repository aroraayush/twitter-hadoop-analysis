package edu.usfca.cs.mr.stack_overflow_highest_rep_users;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * It contains the data for reputed users (class members)
 */
public class RepUserWritable implements Writable {

    private Text displayName;
    private Text website;
    private IntWritable upvotes;
    private IntWritable views;
    private IntWritable reputation;
    private Text creationDate;

    //default constructor for (de)serialization
    public RepUserWritable() {
        this.upvotes = new IntWritable(0);
        this.views = new IntWritable(0);
        this.reputation = new IntWritable(0);
        this.displayName = new Text("");
        this.website = new Text("");
        this.creationDate = new Text("");
    }

    public RepUserWritable(Text displayName,Text website, int upvotes, int views, Text creationDate, int reputation) {
        this.upvotes = new IntWritable(upvotes);
        this.reputation = new IntWritable(reputation);
        this.views = new IntWritable(views);
        this.displayName = new Text(displayName);
        this.website = new Text(website);
        this.creationDate = new Text(creationDate);
    }

    public void write(DataOutput out) throws IOException {
        // Serializing the data to send to next machine
        this.displayName.write(out);
        this.website.write(out);
        this.upvotes.write(out);
        this.views.write(out);
        this.creationDate.write(out);
        this.reputation.write(out);
    }

    public void readFields(DataInput in) throws IOException {
        // deserializing
        this.displayName.readFields(in);
        this.website.readFields(in);
        this.upvotes.readFields(in);
        this.views.readFields(in);
        this.creationDate.readFields(in);
        this.reputation.readFields(in);
    }

    public IntWritable getReputation() {
        return reputation;
    }

    public void setReputation(IntWritable reputation) {
        this.reputation = reputation;
    }

    public Text getDisplayName() {
        return displayName;
    }

    public void setDisplayName(Text displayName) {
        this.displayName = displayName;
    }

    public Text getWebsite() {
        return website;
    }

    public void setWebsite(Text website) {
        this.website = website;
    }

    public IntWritable getUpvotes() {
        return upvotes;
    }

    public void setUpvotes(IntWritable upvotes) {
        this.upvotes = upvotes;
    }

    public IntWritable getViews() {
        return views;
    }

    public void setViews(IntWritable views) {
        this.views = views;
    }

    public Text getCreationDate() {
        return creationDate;
    }

    public void setCreationDate(Text creationDate) {
        this.creationDate = creationDate;
    }

    @Override
    public String toString() {
        return System.lineSeparator() + "\t\t\tUser : " + displayName +
                "\t (since " + creationDate +
                ")\t reputation=" + reputation +
                "\t upvotes=" + upvotes +
                "\t views=" + views +
                "\t website=" + website ;
    }
}