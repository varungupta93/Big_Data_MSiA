import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;


public class Exercise2 extends Configured implements Tool{
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, FloatWritable> {

		
		public void configure(JobConf job) {
		}
	
		protected void setup(OutputCollector<Text, IntWritable> output) throws IOException, InterruptedException {
		}
	
		public void map(LongWritable key, Text value, OutputCollector<Text, FloatWritable> output, Reporter reporter) throws IOException {
		    String line = value.toString();
		    String[] LineArr = line.split(",");
		    
		    
		    if(LineArr[LineArr.length - 1].equals("false")){
		    	Float val = Float.valueOf(LineArr[3]);
			    String keystr = LineArr[29] + "," + LineArr[30] + "," + 
			    				LineArr[31] + "," + LineArr[32];
			    output.collect(new Text(keystr), new FloatWritable(val));
		    }else{
		    	return;
		    }
		   	    	    
		    
		}
	
		protected void cleanup(OutputCollector<Text, FloatWritable> output) throws IOException, InterruptedException {
		}
    }
	 public static class Reduce extends MapReduceBase implements Reducer<Text, FloatWritable, Text, FloatWritable> {

			public void configure(JobConf job) {
			}

			protected void setup(OutputCollector<Text, FloatWritable> output) throws IOException, InterruptedException {
			}

			public void reduce(Text key, Iterator<FloatWritable> values, OutputCollector<Text, FloatWritable> output, Reporter reporter) throws IOException {
			    Float sum = new Float(0.0);
			    int count = 0;
			    while (values.hasNext()) {
			    	sum += values.next().get();
			    	count++;
			    }
			    float avgval = sum.floatValue() / count;
			    output.collect(key, new FloatWritable(avgval));
			}

			protected void cleanup(OutputCollector<Text, FloatWritable> output) throws IOException, InterruptedException {
			}
	 }
	 
	 public int run(String[] args) throws Exception {
			JobConf conf = new JobConf(getConf(), Exercise2.class);
			conf.setJobName("Excercise2");

			// conf.setNumReduceTasks(0);

			// conf.setBoolean("mapred.output.compress", true);
			// conf.setBoolean("mapred.compress.map.output", true);

			conf.setOutputKeyClass(Text.class);
			conf.setOutputValueClass(FloatWritable.class);

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
			int res = ToolRunner.run(new Configuration(), new Exercise2(), args);
			System.exit(res);
	 }
}
