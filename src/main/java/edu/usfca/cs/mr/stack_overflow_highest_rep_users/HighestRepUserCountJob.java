package edu.usfca.cs.mr.stack_overflow_highest_rep_users;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * This is the main class. Hadoop will invoke the main method of this class.
 */
public class HighestRepUserCountJob {
    public static void main(String[] args) {
        try {
            if(args.length < 2){
                System.out.println("Usage <input_file_path> <output_file_path>");
                System.exit(0);
            }
            Configuration conf = new Configuration();

            /* Job Name. You'll see this in the YARN webapp */
            Job job = Job.getInstance(conf, "Location-wise top 5 Users with Highest Reputation, ordered by highest upvotes");

            /* Current class */
            job.setJarByClass(HighestRepUserCountJob.class);

            /* Mapper class */
            job.setMapperClass(HighestRepUserMapper.class);

            /* Combiner class. Combiners are run between the Map and Reduce
             * phases to reduce the amount of output that must be transmitted.
             * In some situations, we can actually use the Reducer as a Combiner
             * but ONLY if its inputs and ouputs match up correctly. The
             * combiner is disabled here, but the following can be uncommented
             * for this particular job:*/
//            job.setCombinerClass(TagCountReducer.class);

            /* Reducer class */
            job.setReducerClass(HightestRepUserReducer.class);

            /* Outputs from the Mapper. */
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(LocationWritable.class);

            /* Outputs from the Reducer */
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(LocationWritable.class);

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
