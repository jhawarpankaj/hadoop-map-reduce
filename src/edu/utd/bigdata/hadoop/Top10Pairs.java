package edu.utd.bigdata.hadoop;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.PriorityQueue;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Top10Pairs {

	public static void main(String[] args)
			throws IOException, ClassNotFoundException, InterruptedException {

		if(args.length != 3) {
			System.err
					.println("Improper arguments. Usage: <input_file> <temp_output> <output_path>");
			System.exit(1);
		}

		// To check the logs at web console, map status etc.

		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://127.0.0.1:9000");
		conf.set("mapreduce.jobtracker.address", "localhost:54311");
		conf.set("mapreduce.framework.name", "yarn");
		conf.set("yarn.resourcemanager.address", "localhost:8032");

		// First job...
		Job job1 = Job.getInstance(conf, "MutualFriends");
		job1.setJarByClass(Top10Pairs.class);
		job1.setMapperClass(MapperClass1.class);
		job1.setReducerClass(ReducerClass1.class);
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));
		if(!job1.waitForCompletion(true)) System.exit(1);

		// Second job...
		Configuration conf2 = new Configuration();
		Job job2 = Job.getInstance(conf2, "Top10");
		conf2.set("K", "10");
		job2.setJarByClass(Top10Pairs.class);
		job2.setMapperClass(MapperClass2.class);
		job2.setReducerClass(ReducerClass2.class);

		job2.setMapOutputKeyClass(NullWritable.class);
		job2.setMapOutputValueClass(Text.class);

		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		job2.setInputFormatClass(KeyValueTextInputFormat.class);

		FileInputFormat.addInputPath(job2, new Path(args[1]));
		FileOutputFormat.setOutputPath(job2, new Path(args[2]));
		System.exit(job2.waitForCompletion(true) ? 0 : 1);
	}

	public static class MapperClass1 extends Mapper<LongWritable, Text, Text, Text> {

		/*
		 * Business Logic for mapper side.
		 * 
		 * @param key The input param key which represents the offset which is not used
		 * in this case.
		 * 
		 * @param values Represents each line of the input file.
		 * 
		 * @param context Mapper context.
		 */
		public void map(LongWritable key, Text values, Context context)
				throws IOException, InterruptedException {
			String[] connections = String.valueOf(values).split("\t");
			if(connections.length != 2) return;
			int myID = Integer.parseInt(connections[0]);
			String[] friendsList = connections[1].split(",");
			Text resultTuple = new Text();
			int friendID;
			for(String friend : friendsList) {
				friendID = Integer.parseInt(friend);
				if(myID < friendID) resultTuple.set(myID + "," + friendID);
				else resultTuple.set(friendID + "," + myID);
				context.write(resultTuple, new Text(connections[1]));
			}
		}
	}

	/**
	 * Reducer class for reduce job. Provides definition of reduce job.
	 * 
	 * @author pankaj
	 *
	 */
	public static class ReducerClass1 extends Reducer<Text, Text, Text, Text> {

		/*
		 * Business Logic for reduce side.
		 * 
		 * @param key The merged unique key produced after the sort phase.
		 * 
		 * @param values Represents the merged list of values for each key.
		 * 
		 * @param context Reducer context.
		 */
		public void reduce(Text key, Iterable<Text> listOfValues, Context context)
				throws IOException, InterruptedException {

			int count = 0;
			Set<String> set = new HashSet<String>();

			for(Text value : listOfValues) {
				String[] split = String.valueOf(value).split(",");
				for(String aFriend : split) {
					if(set.contains(aFriend)) count++;
					else set.add(aFriend);
				}
			}

			if(count == 0) return;
			context.write(new Text(String.valueOf(count)), key);
		}
	}

	/**
	 * For Mapper class Priority queue to maintain top 10 records.
	 * 
	 * @author pankaj
	 *
	 */
	public static class MyComparator implements Comparator<String> {
		@Override
		public int compare(String arg0, String arg1) {
			int a = Integer.parseInt(arg0.split("#")[0]);
			int b = Integer.parseInt(arg1.split("#")[0]);
			return a - b;
		}
	}

	/**
	 * Mapper class for second job. Maintains local top 10 records.
	 * 
	 * @author pankaj
	 *
	 */
	public static class MapperClass2 extends Mapper<Text, Text, NullWritable, Text> {

		private static PriorityQueue<String> PQ = new PriorityQueue<String>(new MyComparator());
		int K;

		/*
		 * Initializing confing variables.
		 */
		protected void setup(Context context) {
			Configuration conf = context.getConfiguration();
			K = conf.getInt("K", 10);
		}

		/*
		 * Business Logic for mapper side.
		 * 
		 * @param key Count of common friends.
		 * 
		 * @param values Pair of friends.
		 * 
		 * @param context Mapper context.
		 */
		public void map(Text key, Text values, Context context)
				throws IOException, InterruptedException {

			String merge = key.toString() + "#" + values.toString();
			PQ.add(merge);
			if(PQ.size() > K) PQ.remove();
		}

		/*
		 * Executed once at the end of each mapper execution to flush top K records.
		 */
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {

			while(!PQ.isEmpty()) {
				context.write(NullWritable.get(), new Text(PQ.poll()));
			}

		}
	}

	/**
	 * Get the global top K records.
	 * 
	 * @author pankaj
	 *
	 */
	public static class ReducerClass2 extends Reducer<NullWritable, Text, Text, Text> {
		int K;

		/*
		 * Initializing confing variables.
		 */
		protected void setup(Context context) {
			Configuration conf = context.getConfiguration();
			K = conf.getInt("K", 10);
		}

		/*
		 * Business Logic for reduce side.
		 * 
		 * @param key Null to send all mapper output to 1 reducer.
		 * 
		 * @param values merged top K records from each mapper.
		 * 
		 * @param context Reducer context.
		 */
		public void reduce(NullWritable key, Iterable<Text> listOfValues, Context context)
				throws IOException, InterruptedException {

			PriorityQueue<String> PQ = new PriorityQueue<String>(new MyComparator());

			for(Text value : listOfValues) {
				PQ.add(value.toString());
				if(PQ.size() > K) PQ.remove();
			}

			String[] array = new String[PQ.size()];
			for(int i = 0; i < array.length; i++) {
				array[i] = PQ.remove();
			}
			Arrays.sort(array, PQ.comparator());
			for(int i = array.length - 1; i >= 0; i--) {
				context.write(new Text(array[i].split("#")[1]), new Text(array[i].split("#")[0]));
			}
		}
	}
}