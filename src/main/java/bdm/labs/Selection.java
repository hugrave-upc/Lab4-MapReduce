package bdm.labs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import javax.rmi.CORBA.Util;

public class Selection extends JobMapReduce {
    
	public Selection() {
		this.input = null;
		this.output = null;
	}

	public static class SelectionMapper extends Mapper<Text, Text, Text, Text> {
		public void map (Text key, Text value, Context context) throws IOException, InterruptedException {
			String filterAttr = context.getConfiguration().getStrings("filterAttr")[0];
			String filterValue = context.getConfiguration().getStrings("filterValue")[0];

			String[] values = value.toString().split(",");
			String actualValue = Utils.getAttribute(values, filterAttr);
			if (filterValue.equals(actualValue)) {
				context.write(key, value);
			}
		}
	}
	
	public boolean run() throws IOException, ClassNotFoundException, InterruptedException {
		Configuration configuration = new Configuration();
		// Define the new job and the name it will be given
		Job job = Job.getInstance(configuration, "Selection");
		configureJob(job,this.input, this.output);
	    // Let's run it!
	    return job.waitForCompletion(true);
	}

    public static void configureJob(Job job, String pathIn, String pathOut) throws IOException, ClassNotFoundException, InterruptedException {
        job.setJarByClass(Selection.class);

        // Configure the rest of parameters required for this job
        // Take a look at the provided examples: Projection, AggregationSum and CartesianProduct
		job.setMapperClass(SelectionMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		FileInputFormat.addInputPath(job, new Path(pathIn));
		FileOutputFormat.setOutputPath(job, new Path(pathOut));

		job.getConfiguration().setStrings("filterAttr", "native_country");
		job.getConfiguration().setStrings("filterValue", "Canada");

    }
}
