
import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class Exercise4 extends Configured implements Tool {

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, DoubleWritable> {

		
		public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
		    String line = value.toString();
		    String[] LineArr = line.split(",");
		    
		    String artist = LineArr[2].trim();
		    double duration = Double.valueOf(LineArr[3].trim()).doubleValue();
		    
		    output.collect(new Text(artist), new DoubleWritable(duration));
		      
		}		
    }
    
    
    public static class Ex4Partitioner extends MapReduceBase implements Partitioner <Text, DoubleWritable >{
    	@Override
    	public int 	getPartition(Text key, DoubleWritable value, int numReduceTasks){
    		String keystr = key.toString();
    		char firstletter = Character.toLowerCase(keystr.charAt(0));
    		if(firstletter< 'f'){
    			return 0;
    		}
    		if((firstletter >= 'f') && (firstletter < 'k')){
    			return 1 % numReduceTasks;
    		}
    		if((firstletter >= 'k') && (firstletter < 'p')){
    			return 2 % numReduceTasks;
    		}
    		if((firstletter >= 'p') && (firstletter < 'u')){
    			return 3 % numReduceTasks;
    		}
    		if(firstletter >= 'u'){
    			return 4 % numReduceTasks;
    		}	
    		return 0;
    	}
    }
    
	 
    public static class Reduce extends MapReduceBase implements Reducer<Text, DoubleWritable, Text, DoubleWritable> {


		public void reduce(Text key, Iterator<DoubleWritable> values, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
		    double maxdur = -100;
		    while (values.hasNext()) {
		    	double val = values.next().get();
		    	if(maxdur < val){
		    		maxdur = val;
		    	}
		    }
		    output.collect(key, new DoubleWritable(maxdur));
		}
    }
	 
	 public int run(String[] args) throws Exception {
			JobConf conf = new JobConf(getConf(), Exercise4.class);
			conf.setJobName("Excercise4");

			conf.setNumReduceTasks(5);

			// conf.setBoolean("mapred.output.compress", true);
			// conf.setBoolean("mapred.compress.map.output", true);

			conf.setOutputKeyClass(Text.class);
			conf.setOutputValueClass(DoubleWritable.class);

			conf.setMapperClass(Map.class);
			conf.setCombinerClass(Reduce.class);
			conf.setReducerClass(Reduce.class);
			conf.setPartitionerClass(Ex4Partitioner.class);	
			conf.setInputFormat(TextInputFormat.class);
			conf.setOutputFormat(TextOutputFormat.class);

			FileInputFormat.setInputPaths(conf, new Path(args[0]));
			FileOutputFormat.setOutputPath(conf, new Path(args[1]));

			JobClient.runJob(conf);
			return 0;
		    }
	 
	 public static void main(String[] args) throws Exception {
			int res = ToolRunner.run(new Configuration(), new Exercise4(), args);
			System.exit(res);
	 }
}
