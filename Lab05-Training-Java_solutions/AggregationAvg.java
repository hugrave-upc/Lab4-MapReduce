package bdm.labs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Set;

public class AggregationAvg extends JobMapReduce {

	public AggregationAvg() {
		this.input = null;
		this.output = null;
	}

	public static class AggregationAvgMapper extends Mapper<Text, Text, Text, DoubleWritable> {
		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			// Obtain the parameters sent during the configuration of the job
			String groupBy = context.getConfiguration().getStrings("groupBy")[0];
			String avg = context.getConfiguration().getStrings("avg")[0];
			// Since the value is a CSV, just get the lines split by commas
			String[] arrayValues = value.toString().split(",");
			String groupByValue = Utils.getAttribute(arrayValues, groupBy);
			double avgValue = Double.parseDouble(Utils.getAttribute(arrayValues, avg));
			// Do the group by and emit it
			context.write(new Text(groupByValue), new DoubleWritable(avgValue));
		}
	}

	public static class AggregationAvgReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
			double sum = 0;
			int numElements = 0;
			for (DoubleWritable value : values) {
				sum += value.get();
				numElements++;
			}
			double avg = sum / numElements;
			context.write(key, new DoubleWritable(avg));
		}
	}
	
	public boolean run() throws IOException, ClassNotFoundException, InterruptedException {
		Configuration configuration = new Configuration();
		// Define the new job and the name it will be given
		Job job = Job.getInstance(configuration, "AggregationAvg");
		AggregationAvg.configureJob(job, this.input, this.output);
	    // Let's run it!
	    return job.waitForCompletion(true);
	}

	public static void configureJob(Job job, String pathIn, String pathOut) throws IOException, ClassNotFoundException, InterruptedException {
		job.setJarByClass(AggregationAvg.class);

		// Configure the rest of parameters required for this job
		// Take a look at the provided examples: Projection, AggregationSum and CartesianProduct
		//mapper
		job.setMapperClass(AggregationAvgMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);

		//no combiner here

		//reducer
		job.setReducerClass(AggregationAvgReducer.class);


		// The output will be Text
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		// The files the job will read from/write to
		job.setInputFormatClass(SequenceFileInputFormat.class);
		FileInputFormat.addInputPath(job, new Path(pathIn));
		FileOutputFormat.setOutputPath(job, new Path(pathOut));
		job.getConfiguration().setStrings("groupBy", "native_country");
		job.getConfiguration().setStrings("avg", "capital_gain");
    }
}
