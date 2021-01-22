package edu.usfca.cs.mr.finalProjectAnalysis2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Comparator;
import java.util.TreeMap;

public class TagsReducer extends Reducer<Text, TagsWritable, Text, TagsWritable> {

    private TreeMap<TagsWritable, Text> treeMap; // using the treeMap to store top 5 tags of stack overflow data

    @Override
    protected void setup(Context context) {
        treeMap = new TreeMap<TagsWritable, Text>(new Comparator<TagsWritable>() {
            // sorting in descending order based on tag Count
            @Override
            public int compare(TagsWritable o1, TagsWritable o2) {
                if(o1.getTagCount().get() > o2.getTagCount().get())
                    return -1;
                else if(o1.getTagCount().get() < o2.getTagCount().get())
                    return  1;
                else
                    return 1;
            }
        });
    }

    @Override
    protected void reduce(
            Text key, Iterable<TagsWritable> values, Context context)
            throws IOException, InterruptedException {

        // extracting data
        TagsWritable tagsWritable = new TagsWritable();
        tagsWritable.setTagName(key);
        for(TagsWritable val : values){
            tagsWritable.setExcerptPostId(val.getExcerptPostId());
            tagsWritable.setWikiPostId(val.getWikiPostId());
            tagsWritable.setTagCount(val.getTagCount().get());

        }
        treeMap.put(tagsWritable, key);
        if(treeMap.size() > 5){
            treeMap.pollLastEntry();
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // final output is tags, the associated data from stack overflow
        for(TagsWritable val : treeMap.keySet()) {
            context.write(val.getTagName(), val);
        }
    }
}
