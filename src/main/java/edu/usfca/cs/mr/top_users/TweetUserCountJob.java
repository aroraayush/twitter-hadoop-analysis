package edu.usfca.cs.mr.top_users;

import edu.usfca.cs.mr.util.MultiLineFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * This is the main class. Hadoop will invoke the main method of this class.
 * This job obtains top 5 users that tweet the most.
 */
public class TweetUserCountJob {
    public static void main(String[] args) {
        try {
            if(args.length < 2){
                System.out.println("Usage <input_file_path> <output_file_path>");
                System.exit(0);
            }
            Configuration conf = new Configuration();

            /* Job Name. You'll see this in the YARN webapp */
            Job job = Job.getInstance(conf, "Top 5 Users job");

            // Ref: https://github.com/ElliottFodi/Hadoop-Programs/tree/master/Hadoop%20multiline%20read
            // Enables multiline reading of lines for the InputRecordReader
            // Reads 4 lines for our case
            job.setInputFormatClass(MultiLineFormat.class);

            /* Current class */
            job.setJarByClass(TweetUserCountJob.class);

            /* Mapper class */
            job.setMapperClass(TweetMapper.class);

            /* Reducer class */
            job.setReducerClass(TweetCountReducer.class);

            /* Outputs from the Mapper. */
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(TweetWritable.class);

            /* Outputs from the Reducer */
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(TweetWritable.class);

            /* Reduce tasks */
            job.setNumReduceTasks(1);

            // Allowing the split size to unlimited / unbounded
            job.getConfiguration().set("mapreduce.job.split.metainfo.maxsize", "-1");

            /* Job input path in HDFS */
            FileInputFormat.addInputPath(job, new Path(args[0]));

            /* Job output path in HDFS. NOTE: if the output path already exists
             * and you try to create it, the job will fail. You may want to
             * automate the creation of new output directories here */
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            /* Wait (block) for the job to complete... */
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
    }
}
