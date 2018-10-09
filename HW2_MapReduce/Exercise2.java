import java.io.*;
import java.util.*;
import java.lang.Math.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


public class Exercise2 extends Configured implements Tool{
	
	public static class oneGramMapper extends Mapper<Object, Text, Text, Text>  {
	
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		    String line = value.toString();
		    String[] LineArr = line.split("\\s+");
		    
		    String val = "";
		    String keystr = "Any";
		    double x = Double.valueOf(LineArr[3]).doubleValue();
		    double xsquare = Math.pow(x,2);
		   	val = "1," + x + ","+ xsquare;
			context.write(new Text(keystr), new Text(val));
    					    	   	
		}
	
    }
	
	
public static class biGramMapper extends Mapper<Object, Text, Text, Text>  {

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	    String line = value.toString();
	    String[] LineArr = line.split("\\s+");
	    
	    String val = "";
	    String keystr = "Any";
	    double x = Double.valueOf(LineArr[4]).doubleValue();
	    double xsquare = Math.pow(x,2);
	   	val = "1," + x + ","+ xsquare;
		context.write(new Text(keystr), new Text(val));					    	   	
	}
	
}

	
	public static class Combine extends Reducer<Text, Text, Text, Text> {
	
	    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	        double Count = 0.0, sumx = 0.0, sumxsquare = 0.0;
	    
	
	        for(Text value : values) {
	        	String[] valssplit = value.toString().split(",");
	        	Count += Double.valueOf(valssplit[0]).doubleValue();
	        	sumx += Double.valueOf(valssplit[1]).doubleValue();
	        	sumxsquare += Double.valueOf(valssplit[2]).doubleValue();	        	        	 
		    }
	        String outputvals = Count + "," + sumx + "," + sumxsquare;
	        context.write(key, new Text(outputvals));
	    }
	}

	public static class Reduce extends Reducer<Text, Text, Text, DoubleWritable> {

			
		 public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			 double Count = 0.0; 
			 double sumx = 0.0;
			 double sumxsquare = 0.0;
			 String keystr = "Standard Deviation: ";
				
		     for(Text value : values) {
		        	String[] valssplit = value.toString().split(",");
		        	Count += Double.valueOf(valssplit[0]).doubleValue();
		        	sumx += Double.valueOf(valssplit[1]).doubleValue();
		        	sumxsquare += Double.valueOf(valssplit[2]).doubleValue();
			 }
		        double standarddev = Math.sqrt((sumxsquare - (Count*(Math.pow((sumx/Count),2))))/Count);
		        context.write(new Text(keystr), new DoubleWritable(standarddev));
			}
		 }
	 
	 public int run(String[] args) throws Exception {
			//JobConf conf = new JobConf(getConf(), Exercise2.class);
			//conf.setJobName("Excercise2");
			
			
			Configuration conf = new Configuration();
			Job job = new Job(conf, "Exercise2");
			job.setJarByClass(Exercise2.class);
			MultipleInputs.addInputPath(job,new Path(args[0]),TextInputFormat.class,oneGramMapper.class);
			MultipleInputs.addInputPath(job,new Path(args[1]),TextInputFormat.class,biGramMapper.class);
			// conf.setNumReduceTasks(0);

			// conf.setBoolean("mapred.output.compress", true);
			// conf.setBoolean("mapred.compress.map.output", true);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(DoubleWritable.class);

			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			//conf.setMapperClass(Map.class);
			job.setCombinerClass(Combine.class);
			job.setReducerClass(Reduce.class);

			//conf.setInputFormat(TextInputFormat.class);
			//conf.setOutputFormat(TextOutputFormat.class);

			FileOutputFormat.setOutputPath(job, new Path(args[2]));

			return (job.waitForCompletion(true) ? 0 : 1);
			
		    }
	 public static void main(String[] args) throws Exception {
			int res = ToolRunner.run(new Configuration(), new Exercise2(), args);
			System.exit(res);
	 }
}