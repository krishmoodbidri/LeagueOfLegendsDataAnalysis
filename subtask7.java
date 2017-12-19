package test2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.json.JSONArray;
import org.json.JSONObject;

public class subtask7
{
    public static void main( String[] args ) throws Exception
    {
    	Configuration conf = new Configuration();
    	Job job = Job.getInstance(conf, "late game champion");
	    job.setJarByClass(subtask7.class);
	    job.setMapperClass(st7Mapper.class);
	    job.setCombinerClass(st7Reducer.class);
	    job.setReducerClass(st7Reducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    job.setNumReduceTasks(20);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);  	    
    }

    static class st7Mapper extends Mapper<Object, Text, Text, IntWritable> {
	
    	private Text champion = new Text();
    	private Text write = new Text();
    	private IntWritable t;

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String jsonString = value.toString();
			JSONObject obj = new JSONObject(jsonString);		
			
				JSONObject data = (JSONObject)obj.get("data");
				JSONArray levelUpEvents = data.getJSONArray("levelUpEvents");
				for (int j = 0; j < levelUpEvents.length(); j++) 
				{
				
					JSONObject o = levelUpEvents.getJSONObject(j);
					
					int unitID = o.getInt("unitID");
					int time = o.getInt("time");
					
					t= new IntWritable(time);
				
					String op = Integer.toString(unitID);
					champion.set(String.valueOf(op));
					write.set(champion);
					context.write(write, t);
				
				}
		}
	}

	static class st7Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable numGames = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

			Integer win = 0;
			int max =0;
			for (IntWritable val : values) {
				if(val.get() > max) max = val.get();
			}
			numGames.set(win);
			context.write(key, numGames);
		}
	}    
}
