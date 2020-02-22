package edu.utd.hadoop.q1;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Finds the list of mutual friends of two users.
 * 
 * @author pankaj
 *
 */
public class FindMutualFriends {

	/**
	 * Main method for entry point.
	 * 
	 * @param args
	 *            Expecting 2 arguments: Input file location and Output path.
	 * @throws IOException
	 *             Occurred during dealing with files read/write.
	 * @throws InterruptedException 
	 * @throws ClassNotFoundException 
	 */
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		if (args.length != 2) {
			System.err.println(
					"Improper arguments. Usage: <input_file> <output_path>");
			System.exit(1);
		}

		// Below configurations to check the logs at web console.
		Configuration conf = new Configuration();		
		conf.set("fs.defaultFS", "hdfs://127.0.0.1:9000");
        conf.set("mapreduce.jobtracker.address", "localhost:54311");
        conf.set("mapreduce.framework.name", "yarn");
        conf.set("yarn.resourcemanager.address", "localhost:8032");
        
        // Hadoop Job's logistics...
		Job job = new Job(conf, "Mutual Friends");
		job.setJarByClass(FindMutualFriends.class);
		job.setMapperClass(MapperClass.class);
		job.setReducerClass(ReducerClass.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);		
		
		// Can be used to check the output of the mapper. Output will be 
		// stored in the location set by setOutputPath below.
		
		// job.setNumReduceTasks(0);

		// Setting input output paths...
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	/**
	 * Mapper class for map job. Provides definition of map job.
	 */
	public static class MapperClass
			extends
				Mapper<LongWritable, Text, Text, Text> {

		/*
		 * Business Logic for mapper side.
		 * 
		 * @param key The input param key which represents the offset 
		 * which is not used in this case.
		 * 
		 * @param values Represents each line of the input file.
		 * 
		 * @param context Mapper context.
		 */
		public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
			String[] connections = String.valueOf(values).split("\t");
			if (connections.length != 2)
				return;
			int myID = Integer.parseInt(connections[0]);
			String[] friendsList = connections[1].split(",");
			Text resultTuple = new Text();
			int friendID;
			for (String friend : friendsList) {
				friendID = Integer.parseInt(friend);
				if (isRequiredPair(myID, friendID)) {
					if (myID < friendID) resultTuple.set(myID + "," + friendID);
					else resultTuple.set(friendID + "," + myID);
					context.write(resultTuple, new Text(connections[1]));
				}
			}			
		}

		/**
		 * @param myId
		 *            ID of the current user.
		 * @param friendId
		 *            Id of the current user's friend.
		 * @return True if as required in question else false.
		 */
		private boolean isRequiredPair(int myID, int friendID) {
			if ((myID == 0 && friendID == 1) || (myID == 1 && friendID == 0)
					|| (myID == 20 && friendID == 28193)
					|| (myID == 28193 && friendID == 20)
					|| (myID == 1 && friendID == 29826)
					|| (myID == 29826 && friendID == 1)
					|| (myID == 6222 && friendID == 19272)
					|| (myID == 19272 && friendID == 6222)
					|| (myID == 28041 && friendID == 28056)
					|| (myID == 28056 && friendID == 28041)) return true;
			return false;
		}
	}

	/**
	 * Reducer class for reduce job. Provides defintion of reduce job.
	 * 
	 * @author pankaj
	 *
	 */
	public static class ReducerClass extends Reducer<Text, Text, Text, Text> {

		/*
		 * Business Logic for reduce side.
		 * 
		 * @param key The merged unique key produced after the sort phase.
		 * 
		 * @param values Represents the merged list of values for each key.
		 * 
		 * @param context Reducer context.
		 */
		public void reduce(Text key, Iterable<Text> listOfValues,
				Context context) throws IOException, InterruptedException {

			StringBuilder mutualFriends = new StringBuilder();
			Text result = new Text();
			Set<String> set = new HashSet<String>();

			for (Text value : listOfValues) {
				String[] split = String.valueOf(value).split(",");
				for (String aFriend : split) {
					if (!set.contains(aFriend)) {
						set.add(aFriend);
						continue;
					}
					mutualFriends.append(aFriend + ",");
				}
			}

			if (String.valueOf(mutualFriends).isEmpty()) return;

			mutualFriends.deleteCharAt(mutualFriends.length() - 1);
			result.set(new Text(String.valueOf(mutualFriends)));
			context.write(key, result);			
		}
	}

}
