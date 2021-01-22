package edu.usfca.cs.mr.finalProjectAnalysis2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.StringTokenizer;


public class TagsMapper extends Mapper<LongWritable, Text, Text, TagsWritable> {
//    private static Logger logger = LoggerFactory.getLogger(SentimentMapper.class.getName());

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        // reading one line at a time
        String line = value.toString();
        // if the line is unusual or contains unusual data, like heading, ignoring that data
        if(line == null || line.isEmpty() || line.trim().length() == 0 || !line.startsWith("  <row"))
            return;


        StringTokenizer itr = new StringTokenizer(line);
        String tagName = "";
        int count = 0;
        int wikiPostId = 0;
        int excerptPostId = 0;
        // extracting information from the data, by breaking into tokens
        while (itr.hasMoreTokens()){
            String token = itr.nextToken();
            if(token.startsWith("TagName")) {
                String[] reg = token.split("\"");
                tagName = reg[1];
            }
            if(token.startsWith("Count")){
                String[] reg = token.split("\"");
                count = Integer.parseInt(reg[1]);
            }
            if(token.startsWith("ExcerptPostId")){
                String[] reg = token.split("\"");
                excerptPostId = Integer.parseInt(reg[1]);
            }
            if(token.startsWith("WikiPostId")){
                String[] reg = token.split("\"");
                wikiPostId = Integer.parseInt(reg[1]);
            }
        }
        // creating custom writable objects and sending to reducer
        TagsWritable tagsWritable = new TagsWritable(new Text(tagName), count, excerptPostId, wikiPostId);
        context.write(new Text(tagName), tagsWritable);
    }

}
