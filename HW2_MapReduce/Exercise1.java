import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


public class Exercise1 extends Configured implements Tool{
	
	public static class oneGramMapper extends Mapper<Object, Text, Text, Text>  {
	
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		    String line = value.toString();
		    String[] LineArr = line.split("\\s+");
		    String[] substrs = {"nu", "chi", "haw"};
		    String substr = "";
		    String val = "";
		    String keystr = "";
		    if(LineArr[1].matches("^\\d{4}$")){
		    	for(int i = 0; i < substrs.length; i++){
		    		substr = substrs[i];
		    		if(LineArr[0].toLowerCase().contains(substr)){
		    			val = LineArr[2] + "," + LineArr[3];
		    			keystr = LineArr[1]+","+substr;
		    			context.write(new Text(keystr), new Text(val));
		    		}
			    	
		    	}
		    	
		    }else{
		    	return;
		    }
		}
	
	
    }
	
	
public static class biGramMapper extends Mapper<Object, Text, Text, Text>  {

	
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		    String line = value.toString();
		    String[] LineArr = line.split("\\s+");
		    String[] substrs = {"nu", "chi", "haw"};
		    String substr = "";
		    String val = "";
		    String keystr = "";
		    if(LineArr[2].matches("^\\d{4}$")){
		    	for(int i = 0; i < substrs.length; i++){
		    		substr = substrs[i];
		    		if(LineArr[0].toLowerCase().contains(substr)){
		    			keystr = LineArr[2] + ","+substr;
		    			val = LineArr[3] + "," + LineArr[4];
		    			context.write(new Text(keystr), new Text(val));
		    		}
		    		if(LineArr[1].toLowerCase().contains(substr)){
		    			keystr = LineArr[2] + ","+substr;
		    			val = LineArr[3] + "," + LineArr[4];
		    			context.write(new Text(keystr), new Text(val));
		    		}
			    	
		    	}
		    	
		    }else{
		    	return;
		    }
		}
	
		
    }

	
	public static class Combine extends Reducer<Text, Text, Text, Text> {
	
	    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	        double sumOccur = 0.0, sumVolumes = 0.0;
	    
	
	        for(Text value : values) {
	        	String[] valssplit = value.toString().split(",");
	        	sumOccur += Double.valueOf(valssplit[0]).doubleValue();
	        	sumVolumes += Double.valueOf(valssplit[1]).doubleValue();
	        		        	        	 
		    }
	        String outputvals = sumOccur + "," + sumVolumes;
	        context.write(key, new Text(outputvals));
	    }
}

	 public static class Reduce extends Reducer<Text, Text, Text, DoubleWritable> {

			
		 public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			 double sumOccur = 0.0; 
			 double sumVolumes = 0.0;
			 String keystr = key.toString();
			 keystr = keystr + ",";
		        for(Text value : values) {
		        	String[] valssplit = value.toString().split(",");
		        	sumOccur += Double.valueOf(valssplit[0]).doubleValue();
		        	sumVolumes += Double.valueOf(valssplit[1]).doubleValue();
		        	
			    }
		        double averageval = sumOccur/sumVolumes;
		        context.write(new Text(keystr), new DoubleWritable(averageval));
			}

		 }
	 
	 public int run(String[] args) throws Exception {
			//JobConf conf = new JobConf(getConf(), Exercise2.class);
			//conf.setJobName("Excercise2");
			
			
			Configuration conf = new Configuration();
			Job job = new Job(conf, "Exercise1");
			job.setJarByClass(Exercise1.class);
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
			int res = ToolRunner.run(new Configuration(), new Exercise1(), args);
			System.exit(res);
	 }
}


