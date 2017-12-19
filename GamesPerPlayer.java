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

public class GamesPerPlayer
{
    public static void main( String[] args ) throws Exception
    {
    	Configuration conf = new Configuration();
    	Job job = Job.getInstance(conf, "Number of games per player");
	    job.setJarByClass(GamesPerPlayer.class);
	    job.setMapperClass(GamesPerPlayerMapper.class);
	    job.setCombinerClass(GamesPerPlayerReducer.class);
	    job.setReducerClass(GamesPerPlayerReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    job.setNumReduceTasks(20);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);  	    
    }

    static class GamesPerPlayerMapper extends Mapper<Object, Text, Text, IntWritable> {
	
		private static final IntWritable one = new IntWritable(1);
		private static final IntWritable zero = new IntWritable(0);
		private Text write = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String jsonString = value.toString();
			JSONObject obj = new JSONObject(jsonString);		
			int winner = obj.getInt("winner");
			
			
			JSONArray players = obj.getJSONArray("players");
			for (int i = 0; i < players.length(); i++) {
				JSONObject o = players.getJSONObject(i);
				
				int teamID = o.getInt("teamID");
				
				int championID = o.getInt("championID");
				
				if(teamID==winner)
				{
				write.set(Integer.toString(championID));
				context.write(write, one);
				}
				else
				{
				write.set(Integer.toString(championID));
				context.write(write, zero);
				}
			}
		}
	}

	static class GamesPerPlayerReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable numGames = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

			Integer win = 0, total = 0, result =0;
			
			for (IntWritable val : values) {
				win += val.get();
				total = total + 1;
			}
			result = (win/total)*100;
			numGames.set(result);
			context.write(key, numGames);
		}
	}    
}
