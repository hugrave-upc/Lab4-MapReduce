package bdm.labs;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.shell.Count;
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

	public static class AverageMapper extends Mapper<Text, Text, Text, CountSum> {
		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			// Group by element becomes the key
			// The value will be a new CountSum object with count = 1 and sum = agg attribute
			String groupBy = context.getConfiguration().getStrings("groupBy")[0];
			String agg = context.getConfiguration().getStrings("agg")[0];
			String[] arrayValues = value.toString().split(",");
			String groupByValue = Utils.getAttribute(arrayValues, groupBy);
			double aggValue = Double.parseDouble(Utils.getAttribute(arrayValues, agg));
			context.write(new Text(groupByValue), new CountSum(1, aggValue));
		}
	}

	public static class AverageCombiner extends Reducer<Text, CountSum, Text, CountSum> {
		public void reduce(Text key, Iterable<CountSum> value, Context context) throws IOException, InterruptedException {
			// Combining all the CountSum objects in only one
			System.out.println(key.toString() + ": " + CountSum.combine(value).toString());
			context.write(key, CountSum.combine(value));
		}
	}

	public static class AverageReducer extends Reducer<Text, CountSum, Text, DoubleWritable> {
		public void reduce(Text key, Iterable<CountSum> value, Context context) throws IOException, InterruptedException {
			// Reduce a list of CountSum objects into the average
			context.write(key, CountSum.reduce(value));
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

		job.setMapperClass(AverageMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(CountSum.class);

		job.setCombinerClass(AverageCombiner.class);
		job.setReducerClass(AverageReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		FileInputFormat.addInputPath(job, new Path(pathIn));
		FileOutputFormat.setOutputPath(job, new Path(pathOut));

		job.getConfiguration().setStrings("groupBy", "native_country");
		job.getConfiguration().setStrings("agg", "capital_gain");


    }

    public static class CountSum {
		public int count;
		public double sum;

		public CountSum(int count, double sum ) {
			this.count = count;
			this.sum = sum;
		}

		public static CountSum combine (Iterable<CountSum> values) {
			int count = 0;
			double sum = 0;
			for (CountSum cs : values) {
				count += cs.count;
				sum += cs.sum;
			}
			return new CountSum(count, sum);
		}

		public static DoubleWritable reduce(Iterable<CountSum> values) {
			int count = 0;
			double sum = 0;
			for (CountSum cs : values) {
				count += cs.count;
				sum += cs.sum;
			}
			return new DoubleWritable(sum/count);
		}

		public String toString() {
			return "count: " + String.valueOf(this.count) + ", sum: " + String.valueOf(this.sum);
		}
	}
}
