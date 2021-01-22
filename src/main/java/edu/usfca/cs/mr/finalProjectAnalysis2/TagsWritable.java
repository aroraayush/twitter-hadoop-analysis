package edu.usfca.cs.mr.finalProjectAnalysis2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TagsWritable implements Writable{
// this custom writable is for analyzing the data of stack overflow, tags file
    private Text tagName;
    private IntWritable tagCount;
    private IntWritable excerptPostId;
    private IntWritable wikiPostId;


    public TagsWritable(){
        this.tagName = new Text("");
        this.tagCount = new IntWritable(0);
        this.excerptPostId = new IntWritable(0);
        this.wikiPostId = new IntWritable(0);
    }

    public TagsWritable(Text tagName, int tagCount, int excerptPostId, int wikiPostId){
       this.tagName = new Text(tagName);
       this.tagCount = new IntWritable(tagCount);
       this.excerptPostId = new IntWritable(excerptPostId);
       this.wikiPostId = new IntWritable(wikiPostId);
    }

    public Text getTagName() {
        return tagName;
    }

    public void setTagName(Text tagName) {
        this.tagName = new Text(tagName);
    }

    public IntWritable getTagCount() {
        return tagCount;
    }

    public void setTagCount(int tagCount) {
        this.tagCount = new IntWritable(this.tagCount.get() + tagCount);
    }

    public IntWritable getExcerptPostId() {
        return excerptPostId;
    }

    public void setExcerptPostId(IntWritable excerptPostId) {
        this.excerptPostId = excerptPostId;
    }

    public IntWritable getWikiPostId() {
        return wikiPostId;
    }

    public void setWikiPostId(IntWritable wikiPostId) {
        this.wikiPostId = wikiPostId;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        this.tagName.write(dataOutput);
        this.wikiPostId.write(dataOutput);
        this.excerptPostId.write(dataOutput);
        this.tagCount.write(dataOutput);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.tagName.readFields(dataInput);
        this.wikiPostId.readFields(dataInput);
        this.excerptPostId.readFields(dataInput);
        this.tagCount.readFields(dataInput);
    }

    @Override
    public String toString() {
        return "TagsWritable{" +
                "tagName=" + tagName +
                ", tagCount=" + tagCount +
                ", excerptPostId=" + excerptPostId +
                ", wikiPostId=" + wikiPostId +
                '}';
    }
}
