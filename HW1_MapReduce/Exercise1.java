
import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class Exercise1 extends Configured implements Tool {

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
	
		public void configure(JobConf job) {
		}
	
		protected void setup(OutputCollector<Text, IntWritable> output) throws IOException, InterruptedException {
		}
	
		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
		    String line = value.toString();
		    String year = line.substring(15, 19);
		    Integer temperature = Integer.valueOf(line.substring(88,92));
		    final int[] goodquals = {0,1,4,5,9};
		    int qual = Character.getNumericValue(line.charAt(92)); 
		    if(temperature.equals(9999)|| (Arrays.binarySearch(goodquals, qual) < 0) ){
		    	return;
		    }
		    
		    if(line.charAt(87) == '-'){
		    	temperature = -1 * temperature ;
		    }
		    
			output.collect(new Text(year), new IntWritable(temperature));
		    
		}
	
		protected void cleanup(OutputCollector<Text, IntWritable> output) throws IOException, InterruptedException {
		}
    }
	 public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

			public void configure(JobConf job) {
			}

			protected void setup(OutputCollector<Text, IntWritable> output) throws IOException, InterruptedException {
			}

			public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			    int maxtemp = -10000;
			    while (values.hasNext()) {
			    	int val = values.next().get();
			    	if(maxtemp < val){
			    		maxtemp = val;
			    	}
			    }
			    output.collect(key, new IntWritable(maxtemp));
			}

			protected void cleanup(OutputCollector<Text, IntWritable> output) throws IOException, InterruptedException {
			}
	 }
	 
	 public int run(String[] args) throws Exception {
			JobConf conf = new JobConf(getConf(), Exercise1.class);
			conf.setJobName("Excercise1");

			// conf.setNumReduceTasks(0);

			// conf.setBoolean("mapred.output.compress", true);
			// conf.setBoolean("mapred.compress.map.output", true);

			conf.setOutputKeyClass(Text.class);
			conf.setOutputValueClass(IntWritable.class);

			conf.setMapperClass(Map.class);
			conf.setCombinerClass(Reduce.class);
			conf.setReducerClass(Reduce.class);

			conf.setInputFormat(TextInputFormat.class);
			conf.setOutputFormat(TextOutputFormat.class);

			FileInputFormat.setInputPaths(conf, new Path(args[0]));
			FileOutputFormat.setOutputPath(conf, new Path(args[1]));

			JobClient.runJob(conf);
			return 0;
		    }
	 public static void main(String[] args) throws Exception {
			int res = ToolRunner.run(new Configuration(), new Exercise1(), args);
			System.exit(res);
	 }
}
 
