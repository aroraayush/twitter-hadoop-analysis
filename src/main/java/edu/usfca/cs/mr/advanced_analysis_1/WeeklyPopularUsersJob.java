package edu.usfca.cs.mr.advanced_analysis_1;

import edu.usfca.cs.mr.util.MultiLineFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * This is the main class. Hadoop will invoke the main method of this class.
 * This obtains the top 5 weekly popular users & sentiment along the tweets
 * in which these popular users were tagged
 */
public class WeeklyPopularUsersJob {
    public static void main(String[] args) {
        try {
            if(args.length < 4){
                System.out.println("Usage <input_file_path> <output_file_path> <positive_word_txt_path> <negative_word_txt_path>");
                System.exit(0);
            }
            Configuration conf = new Configuration();
            conf.setStrings("positive_word_txt_path", args[2]);
            /* Set up the negative_word_txt_path for mappers: */
            conf.setStrings("negative_word_txt_path", args[3]);

            /* Job Name. You'll see this in the YARN webapp */
            Job job1 = Job.getInstance(conf, "Top 5 popular users");

            // Ref: https://github.com/ElliottFodi/Hadoop-Programs/tree/master/Hadoop%20multiline%20read
            // Enables multiline reading of lines for the InputRecordReader
            // Reads 4 lines for our case
            job1.setInputFormatClass(MultiLineFormat.class);

            /* Current class */
            job1.setJarByClass(WeeklyPopularUsersJob.class);

            /* Mapper class */
            job1.setMapperClass(WeeklyPopUsersMapper.class);

            /* Reducer class */
            job1.setReducerClass(WeeklyPopUsersReducer.class);

            /* Outputs from the Mapper. */
            job1.setMapOutputKeyClass(IntWritable.class);
            job1.setMapOutputValueClass(WeeklyUsersWritable.class);

            /* Outputs from the Reducer */
            job1.setOutputKeyClass(Text.class);
            job1.setOutputValueClass(WeeklyUsersWritable.class);

            /* Reduce tasks */
            job1.setNumReduceTasks(1);

            // Allowing the split size to unlimited / unbounded
            job1.getConfiguration().set("mapreduce.job.split.metainfo.maxsize", "-1");

            /* Job input path in HDFS */
            FileInputFormat.addInputPath(job1, new Path(args[0]));

            /* Job output path in HDFS. */
            FileOutputFormat.setOutputPath(job1, new Path(args[1]));

            /* Wait (block) for the job to complete... */
            System.exit(job1.waitForCompletion(true) ? 0 : 1);

        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
    }
}
