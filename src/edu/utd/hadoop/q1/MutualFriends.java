package edu.utd.hadoop.q1;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;					
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class MutualFriends{

	public static class MapClass extends Mapper<LongWritable, Text, Text, Text> {

			public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {

				String[] friends = values.toString().split("\t");

				if (friends.length == 2) {

					int friend1ID = Integer.parseInt(friends[0]);
					String[] friendsList = friends[1].split(",");
					int friend2ID;
					Text tuple_key = new Text();
					for (String friend2 : friendsList) {
						friend2ID = Integer.parseInt(friend2);

						if((friend1ID==0 && friend2ID ==1 )||(friend1ID==1 && friend2ID ==0 )||(friend1ID==20 && friend2ID ==28193 )||(friend1ID==28193 && friend2ID ==20 )||(friend1ID==1 && friend2ID ==29826 )||(friend1ID==29826 && friend2ID ==1 )||(friend1ID==6222 && friend2ID ==19272 )||(friend1ID==19272 && friend2ID ==6222 )||(friend1ID==28041 && friend2ID == 28056)||(friend1ID==28056 && friend2ID ==28041)){
							if (friend1ID < friend2ID) {
								tuple_key.set(friend1ID + "," + friend2ID);
							} else {
								tuple_key.set(friend2ID + "," + friend1ID);
							}
							context.write(tuple_key, new Text(friends[1]));
						}
					}
				}
			}
		}


    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
		public void reduce (Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			HashMap<String, Integer> friendsHashMap = new HashMap<String,Integer>();
			StringBuilder commonFriendLine = new StringBuilder();
			Text commonFriend = new Text();
			for (Text tuples : values) {
				String[] friendsList = tuples.toString().split(",");
				for (String eachFriend : friendsList) {
					if(friendsHashMap.containsKey(eachFriend)){
						commonFriendLine.append(eachFriend+",");
					}else {
						friendsHashMap.put(eachFriend, 1);
					}
				}
			}
			if(commonFriendLine.length()>0) {
				commonFriendLine.deleteCharAt(commonFriendLine.length()-1);
			}
			commonFriend.set(new Text(commonFriendLine.toString()));
			context.write(key, commonFriend);
		}

	}


    public static void main(String[] args) throws Exception{
		if(args.length!=2) {
			System.out.println("Usage: <Common friends input file path> <output_path>");
			System.exit(1);
		}

		Configuration conf = new Configuration();

//		Job job = Job.getInstance(conf, "Mutual Friends");
		Job job = new Job(conf, "Mutual Friends");
		job.setJarByClass(MutualFriends.class);
		job.setMapperClass(MapClass.class);
		job.setReducerClass(ReducerClass.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}