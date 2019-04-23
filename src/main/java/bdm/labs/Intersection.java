package bdm.labs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Intersection extends JobMapReduce {

	public Intersection() {
		this.input = null;
		this.output = null;
	}

	public static class IntersectionMapper extends Mapper<Text, Text, Text, Text> {
		public void map (Text key, Text value, Context context) throws IOException, InterruptedException {
			String tableTag = context.getConfiguration().getStrings("tableTag")[0];
			String[] countries = context.getConfiguration().getStrings("countries");

			String[] values = value.toString().split(",");
			final String row_country = Utils.getAttribute(values, tableTag);

			boolean skipRow = true;
			for (String c : countries) {
				if (c.equals(row_country)) {
					skipRow = false;
				}
			}
			if (skipRow)
				return;

			// The country of the adult is one of the considered ones.
			// Remove the country now
			StringBuilder newKey = new StringBuilder();
			for (String v : values) {
				if (v.equals(row_country))
					continue;
				newKey.append(v + ",");
			}
			String newKeyString = newKey.toString().substring(0, newKey.toString().length()-2);
			context.write(new Text(newKeyString), new Text(row_country));

		}
	}

	public static class IntersectionReducer extends Reducer<Text, Text, Text, NullWritable> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String[] countries = context.getConfiguration().getStrings("countries");
			Map<String, Integer> counters = new HashMap<String, Integer>();

			for (Text value : values) {
				counters.put(value.toString(), 1);
			}
			int counter = 0;
			for (Integer i : counters.values()) {

			}
		}
	}
	
	public boolean run() throws IOException, ClassNotFoundException, InterruptedException {
		Configuration configuration = new Configuration();
		// Define the new job and the name it will be given
		Job job = Job.getInstance(configuration, "Intersection");
		configureJob(job,this.input, this.output);
	    // Let's run it!
	    return job.waitForCompletion(true);
	}

    public static void configureJob(Job job, String pathIn, String pathOut) throws IOException, ClassNotFoundException, InterruptedException {
        job.setJarByClass(Intersection.class);

        // Configure the rest of parameters required for this job
        // Take a look at the provided examples: Projection, AggregationSum and CartesianProduct
		job.setMapperClass(IntersectionMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		FileInputFormat.addInputPath(job, new Path(pathIn));
		FileOutputFormat.setOutputPath(job, new Path(pathOut));

		job.getConfiguration().setStrings("tableTag", "native_country");
		job.getConfiguration().setStrings("countries", "Italy", "Japan");

    }
}
