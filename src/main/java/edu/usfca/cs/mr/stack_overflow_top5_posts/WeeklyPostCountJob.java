package edu.usfca.cs.mr.stack_overflow_top5_posts;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * This is the main class. Hadoop will invoke the main method of this class.
 */
public class WeeklyPostCountJob {
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
            Job job = Job.getInstance(conf, "Weekly Posts Job");

            /* Current class */
            job.setJarByClass(WeeklyPostCountJob.class);

            /* Mapper class */
            job.setMapperClass(PostsMapper.class);

            /* Combiner class. Combiners are run between the Map and Reduce
             * phases to reduce the amount of output that must be transmitted.
             * In some situations, we can actually use the Reducer as a Combiner
             * but ONLY if its inputs and ouputs match up correctly. The
             * combiner is disabled here, but the following can be uncommented
             * for this particular job:*/
//            job.setCombinerClass(TagCountReducer.class);

            /* Reducer class */
            job.setReducerClass(PostCountReducer.class);

            /* Outputs from the Mapper. */
            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(WeeklyWritable.class);

            /* Outputs from the Reducer */
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(WeeklyWritable.class);

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
