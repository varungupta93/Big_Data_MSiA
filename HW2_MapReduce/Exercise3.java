import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class Exercise3 extends Configured implements Tool {

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

		
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		    String line = value.toString();
		    String[] LineArr = line.split(",");
		    String year = LineArr[165].trim();
		    String title = LineArr[0].trim();
		    String artist = LineArr[2].trim();
		    String duration = LineArr[3].trim();
		    
		    if(year.matches("^\\d{4}$")){
		    	if((Integer.valueOf(year).intValue() > 1999) && (Integer.valueOf(year).intValue()<2011)){
		    		String val = title + "," + artist + "," + duration;
		    		String keystr = "";
		    		output.collect(new Text(keystr), new Text(val));
		    	}
		    	
		    }
		      
		}		
    }
	 
	 
	 public int run(String[] args) throws Exception {
			JobConf conf = new JobConf(getConf(), Exercise3.class);
			conf.setJobName("Excercise3");

			conf.setNumReduceTasks(0);

			// conf.setBoolean("mapred.output.compress", true);
			// conf.setBoolean("mapred.compress.map.output", true);

			conf.setOutputKeyClass(Text.class);
			conf.setOutputValueClass(IntWritable.class);

			conf.setMapperClass(Map.class);
			//conf.setCombinerClass(Reduce.class);
			//conf.setReducerClass(Reduce.class);

			conf.setInputFormat(TextInputFormat.class);
			conf.setOutputFormat(TextOutputFormat.class);

			FileInputFormat.setInputPaths(conf, new Path(args[0]));
			FileOutputFormat.setOutputPath(conf, new Path(args[1]));

			JobClient.runJob(conf);
			return 0;
		    }
	 
	 public static void main(String[] args) throws Exception {
			int res = ToolRunner.run(new Configuration(), new Exercise3(), args);
			System.exit(res);
	 }
}