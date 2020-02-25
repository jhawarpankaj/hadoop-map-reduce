package edu.utd.bigdata.hadoop;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author pankaj
 * Get top 10 max age of friends.
 */
public class Top10MaxAgeUsers {
	
	/**
	 * @param args <connections_file> <user_data> <temp_output_path> <output_path>
	 * @throws IOException standard hadoop job exceptions.
	 * @throws ClassNotFoundException standard hadoop job exceptions.
	 * @throws InterruptedException standard hadoop job exceptions.
	 */
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		if(args.length != 4) {
			System.err.println("Improper arguments. Usage: <connections_file>"
					+ "<user_data> <temp_output_path> <output_path>");
			System.exit(1);
		}
		
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://127.0.0.1:9000");
		conf.set("mapreduce.jobtracker.address", "localhost:54311");
		conf.set("mapreduce.framework.name", "yarn");
		conf.set("yarn.resourcemanager.address", "localhost:8032");
		
		// Job1... (reduce side join implemented here.)
		Job job1 = Job.getInstance(conf, "ReduceJoin1");
		job1.setJarByClass(Top10MaxAgeUsers.class);
		MultipleInputs.addInputPath(job1, new Path(args[0]), TextInputFormat.class,
				MapperClass1.class);
		MultipleInputs.addInputPath(job1, new Path(args[1]), TextInputFormat.class,
				MapperClass2.class);
		job1.setReducerClass(ReducerClass1.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		job1.addCacheFile(new Path(args[1]).toUri());
		FileOutputFormat.setOutputPath(job1, new Path(args[2]));
		if(!job1.waitForCompletion(true)) System.exit(1);
		
		// Job2... (extracting global top 10)
		Job job2 = Job.getInstance(new Configuration(), "ReduceJoin2");
		job2.setJarByClass(Top10MaxAgeUsers.class);
		FileInputFormat.addInputPath(job2, new Path(args[2]));		
		job2.setMapOutputKeyClass(NullWritable.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setMapperClass(MapperClass3.class);        
        job2.setReducerClass(ReducerClass2.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(NullWritable.class);
        FileOutputFormat.setOutputPath(job2, new Path(args[3]));
        if(!job2.waitForCompletion(true)) System.exit(1);
		
	}
	
	/**
	 * @author pankaj
	 * Load connection dataset with the join key(userid) as output key.
	 */
	private static class MapperClass1 extends Mapper<LongWritable, Text, Text, Text> {
		
		private static final String fileTag = "MF~";
		
		/* 
		 * Load connection dataset with fileTag MF~.
		 */
		public void map(LongWritable offset, Text values, Context context)
				throws IOException, InterruptedException {
			
			String[] connections = String.valueOf(values).split("\t");
			if(connections.length != 2) return;
			context.write(new Text(connections[0]), new Text(fileTag + connections[1]));
		}
	}

	/**
	 * @author pankaj
	 * Load userdata dataset with the join key(userid) as output key.
	 */
	private static class MapperClass2 extends Mapper<LongWritable, Text, Text, Text> {
		
		private static final String fileTag = "UD~";

		/* 
		 * Load userdata dataset with fileTag UD~.
		 */
		public void map(LongWritable offset, Text values, Context context)
				throws IOException, InterruptedException {

			String[] data = String.valueOf(values).split(",");
			if(data.length != 10) return;
			String address = data[3] + "," + data[4] + "," + data[5];
			context.write(new Text(data[0]), new Text(fileTag + address));
		}
	}
	
	/**
	 * @author pankaj
	 * Reduce side join to join the connections and userdata on key: userid.
	 */
	private static class ReducerClass1 extends Reducer<Text, Text, Text, Text> {
		
		private static Map<String, Integer> map = new HashMap<String, Integer>();
		private static final String fileTag1 = "MF";
		private static final String fileTag2 = "UD";
		
		/**
		 * Calculate age given DOB.
		 * @param stringDate Input date.
		 * @return Age.
		 */
		private int calculateAge(String stringDate) {

			Calendar today = Calendar.getInstance();
			SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy");
			Date date;
			try {
				date = sdf.parse(stringDate);
			} catch(ParseException e) {
				System.err.println("Improper date format.");
				return 0;
			}
			Calendar dob = Calendar.getInstance();
			dob.setTime(date);

			int curYear = today.get(Calendar.YEAR);
			int dobYear = dob.get(Calendar.YEAR);
			int age = curYear - dobYear;

			int curMonth = today.get(Calendar.MONTH);
			int dobMonth = dob.get(Calendar.MONTH);
			if(dobMonth > curMonth) age--;
			else if(dobMonth == curMonth) { 
				int curDay = today.get(Calendar.DAY_OF_MONTH);
				int dobDay = dob.get(Calendar.DAY_OF_MONTH);
				if(dobDay > curDay) age--;
			}
			return age;
		}
		
		/* 
		 * Load userid and DOB in memory.
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
					map.put(cols[0].trim(), calculateAge(cols[9]));
				}
				line = reader.readLine();
			}
			reader.close();
		}
		
		/* 
		 * Calculate max age among all friends and store address.
		 */
		public void reduce(Text key, Iterable<Text> listOfValues, Context context)
				throws IOException, InterruptedException {
			
			String conn = "", add = "";
			for(Text value : listOfValues) {
				String[] split = String.valueOf(value).split("~");
				if(split[0].equals(fileTag1)) conn += split[1];
				else if(split[0].equals(fileTag2)) add += split[1];
			}
			if(conn.isEmpty() || add.isEmpty()) return;
			
			int maxAge = Integer.MIN_VALUE;
			for(String friend : conn.split(",")) {
				if(!map.containsKey(friend)) continue;
				int age = map.get(friend);
				maxAge = Math.max(maxAge, age);
			}
			
			context.write(key, new Text(maxAge + "#" + add));
		} 
	}
	
	/**
	 * @author pankaj
	 * Bigger age pushed to end.
	 */
	public static class MyComparator implements Comparator<String> {
		@Override
		public int compare(String arg0, String arg1) {
			int a = Integer.parseInt(arg0.split("#")[1]);
			int b = Integer.parseInt(arg1.split("#")[1]);
			return a - b;
		}
	}
	
	/**
	 * @author pankaj
	 * Eject local top 10 highest age.
	 */
	private static class MapperClass3 extends Mapper<LongWritable, Text, NullWritable, Text> {

		private static PriorityQueue<String> PQ = new PriorityQueue<String>(new MyComparator());

		public void map(LongWritable offset, Text values, Context context)
				throws IOException, InterruptedException {

			String[] data = String.valueOf(values).split("\t");
			if(data.length != 2) return;
			String merge = data[0] + "#" + data[1];
			PQ.add(merge);
			if(PQ.size() > 10) PQ.remove();
		}

		/* 
		 * Output only top 10.
		 */
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {

			while(!PQ.isEmpty()) {
				context.write(NullWritable.get(), new Text(PQ.poll()));
			}
		}
	}
	
	/**
	 * @author pankaj
	 * Eject global top 10 max age.
	 */
	public static class ReducerClass2 extends Reducer<NullWritable, Text, Text, NullWritable> {
		
		public void reduce(NullWritable key, Iterable<Text> listOfValues, Context context)
				throws IOException, InterruptedException {

			PriorityQueue<String> PQ = new PriorityQueue<String>(new MyComparator());

			for(Text value : listOfValues) {
				PQ.add(value.toString());
				if(PQ.size() > 10) PQ.remove();
			}

			String[] array = new String[PQ.size()];
			for(int i = 0; i < array.length; i++) {
				array[i] = PQ.remove();
			}
			Arrays.sort(array, PQ.comparator());
			for(int i = array.length - 1; i >= 0; i--) {
				String userID = array[i].split("#")[0];
				String age = array[i].split("#")[1];
				String address = array[i].split("#")[2];
				context.write(new Text(userID + "," + address + "," + age), NullWritable.get());
			}
		}
	}
}
