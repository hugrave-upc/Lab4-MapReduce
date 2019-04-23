package bdm.labs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class ProjectionCSV extends JobMapReduce {

    public ProjectionCSV() {
        this.input = null;
        this.output = null;
    }

    public static class ProjectionCSVMapper extends Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Obtain the parameters sent during the configuration of the job
            String[] projection = context.getConfiguration().getStrings("projection");
            if (!value.toString().contains("workclass")) {
                // Since the value is a CSV, just get the lines split by commas
                String[] values = value.toString().split(",");
                String projectionValue = Utils.getAttribute(values, projection[0]);
                // Get the CSV position of the attributes, do the projection and emit it
                StringBuilder newValue = new StringBuilder(projectionValue);
                for (int i = 1; i < projection.length; i++) {
                    projectionValue = Utils.getAttribute(values, projection[i]);
                    newValue.append("," + projectionValue);
                }
                context.write(new Text(key.toString()), new Text(newValue.toString()));
            }
        }
    }

    public boolean run() throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();

        // Define the new job and the name it will be given
        Job job = Job.getInstance(configuration, "ProjectionFromCSV");
        ProjectionCSV.configureJob(job,this.input,this.output);
        // Let's run it!
        return job.waitForCompletion(true);
    }

    public static void configureJob(Job job, String pathIn, String pathOut) throws IOException, ClassNotFoundException, InterruptedException {
        job.setJarByClass(ProjectionCSV.class);
        // Set the mapper class it must use
        job.setMapperClass(ProjectionCSVMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        // No combiner or reducer classes for this example

        // The output will be Text, Text
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        // The files and formats the job will read from/write to
        FileInputFormat.addInputPath(job, new Path(pathIn));
        FileOutputFormat.setOutputPath(job, new Path(pathOut));
        // These are the parameters that we are sending to the job
        job.getConfiguration().setStrings("projection", "age", "relationship", "native_country");
    }

}
