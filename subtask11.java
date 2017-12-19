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

public class subtask11
{
    public static void main( String[] args ) throws Exception
    {
    	Configuration conf = new Configuration();
    	Job job = Job.getInstance(conf, "Damage recieved per champion");
	    job.setJarByClass(subtask11.class);
	    job.setMapperClass(st5Mapper.class);
	    job.setCombinerClass(st5Reducer.class);
	    job.setReducerClass(st5Reducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    job.setNumReduceTasks(20);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);  	    
    }

    static class st5Mapper extends Mapper<Object, Text, Text, IntWritable> {
	
    	private Text champion = new Text();
    	private Text write = new Text();
    	private IntWritable dam;

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String jsonString = value.toString();
			JSONObject obj = new JSONObject(jsonString);		
			
				JSONObject data = (JSONObject)obj.get("data");
				JSONArray damageEvents = data.getJSONArray("damageEvents");
				for (int j = 0; j < damageEvents.length(); j++) 
				{
				
					JSONObject o = damageEvents.getJSONObject(j);
					int championID = o.getInt("receiverUnitID");
					int damage = o.getInt("damage");
					int res = damage;
					
					dam= new IntWritable(res);
				
					String op = Integer.toString(championID);
					champion.set(String.valueOf(op));
					write.set(champion);
					context.write(write, dam);
				
				}
		}
	}

	static class st5Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable numGames = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

			Integer win = 0;
			
			for (IntWritable val : values) {
				win += val.get();
			}
			numGames.set(win);
			context.write(key, numGames);
		}
	}    
}
