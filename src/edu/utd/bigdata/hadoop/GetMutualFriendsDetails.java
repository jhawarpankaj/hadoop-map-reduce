package edu.utd.bigdata.hadoop;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
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
 * @author pankaj Get name and data of birth for the mutual friends.
 */
public class GetMutualFriendsDetails {

	/**
	 * @param args <connections_file> <user_data> <output_path> <user_1> <user_2>
	 */
	public static void main(String[] args)
			throws IOException, ClassNotFoundException, InterruptedException {

		if(args.length != 5) {
			System.err.println("Insufficient parameters. Usage: <connections_file> "
					+ " <user_data> <output_path> <user1> <user2>");
			System.exit(1);
		}

		// Job configurations...
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://127.0.0.1:9000");
		conf.set("mapreduce.jobtracker.address", "localhost:54311");
		conf.set("mapreduce.framework.name", "yarn");
		conf.set("yarn.resourcemanager.address", "localhost:8032");
		conf.set("userID1", args[3]);
		conf.set("userID2", args[4]);

		// Job initializations...
		Job job = Job.getInstance(conf, "MapJoin");
		job.setJarByClass(GetMutualFriendsDetails.class);
		job.setMapperClass(MapperClass.class);
		job.setReducerClass(ReducerClass.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.addCacheFile(new Path(args[1]).toUri()); // cache file for join (present on local file
														// system).

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

	/**
	 * @author pankaj Get details of the users' connection's by doing a map side
	 *         join.
	 */
	private static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {

		/**
		 * To store the details of the required users in memory.
		 */
		private static Map<Integer, String> map = new HashMap<Integer, String>();

		/*
		 * To build an in memory map from the details file. All map functions have this
		 * details file available in memory to perform a join on.
		 */
		protected void setup(Context context) throws IOException, InterruptedException {

			super.setup(context);
			if(context.getCacheFiles() == null || context.getCacheFiles().length == 0)
				throw new IllegalArgumentException("User data file not found.");

			URI[] localPaths = context.getCacheFiles();
			Path cachedFile = new Path(localPaths[0]);
			BufferedReader reader = new BufferedReader(new FileReader(cachedFile.toString()));
			String line = reader.readLine();
			while(line != null) {
				String[] cols = line.split(",");
				if(cols.length == 10) {
					map.put(Integer.parseInt(cols[0]), cols[1] + ":" + cols[9]);
				}
				line = reader.readLine();
			}
			reader.close();
		}

		/*
		 * Business logic for mapper function to get details of the required pairs by
		 * performing a join.
		 */
		public void map(LongWritable key, Text values, Context context)
				throws IOException, InterruptedException {

			Configuration conf = context.getConfiguration();
			int userID1 = Integer.parseInt(conf.get("userID1"));
			int userID2 = Integer.parseInt(conf.get("userID2"));

			String[] connections = String.valueOf(values).split("\t");
			if(connections.length != 2) return;
			int myID = Integer.parseInt(connections[0]);
			String[] friendsList = connections[1].split(",");
			Text resultKey = new Text();
			int friendID;
			StringBuilder sb;
			for(String friend : friendsList) {
				friendID = Integer.parseInt(friend);
				if((myID == userID1 && friendID == userID2)
						|| (myID == userID2 && friendID == userID1)) {
					sb = new StringBuilder();
					if(myID < friendID) resultKey.set(myID + "," + friendID);
					else resultKey.set(friendID + "," + myID);
					for(String conn : friendsList) {
						sb.append(conn + ":" + map.get(Integer.parseInt(conn)) + ",");
					}
					if(sb.length() > 0) sb.deleteCharAt(sb.length() - 1);
					context.write(resultKey, new Text(sb.toString()));
				}
			}
		}
	}

	/**
	 * @author pankaj Get required details of the mutual friends of the given users.
	 */
	private static class ReducerClass extends Reducer<Text, Text, Text, Text> {

		/*
		 * Business logic to extract name and DOB of the mutual friends.
		 */
		public void reduce(Text key, Iterable<Text> listOfValues, Context context)
				throws IOException, InterruptedException {

			StringBuilder mutualFriends = new StringBuilder();
			Text result = new Text();
			Set<String> set = new HashSet<String>();

			for(Text value : listOfValues) {
				String[] split = String.valueOf(value).split(",");
				for(String aFriend : split) {
					String[] details = aFriend.split(":");
					if(!set.contains(details[0])) {
						set.add(details[0]);
						continue;
					}
					mutualFriends.append(details[1] + ":" + details[2] + ",");
				}
			}

			if(String.valueOf(mutualFriends).isEmpty()) return;

			mutualFriends.deleteCharAt(mutualFriends.length() - 1);
			mutualFriends.insert(0, "[");
			mutualFriends.append("]");
			result.set(new Text(String.valueOf(mutualFriends)));
			context.write(key, result);
		}
	}
}
