package com.txy;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import java.util.*;

import java.io.IOException;
public class CommonFriend {
	static class MutualFriendsMapper extends Mapper<LongWritable, Text, Text, Text>{
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] userFriends = line.split(", ");
			String user = userFriends[0];
			String[] friends = userFriends[1].split(" ");
			
			for(String friend : friends){
				context.write(new Text(friend), new Text(user));
			}
		}
	}

	static class MutualFriendsReducer extends Reducer<Text, Text, Text, Text>{
		@Override
		protected void reduce(Text key, Iterable<Text> values,Context context)
				throws IOException, InterruptedException {
			StringBuffer sb = new StringBuffer();
			for(Text friend : values){
				sb.append(friend.toString()).append(",");
			}
			sb.deleteCharAt(sb.length()-1);
			context.write(key, new Text(sb.toString()));
		}
	}


	static class FindFriendsMapper extends Mapper<LongWritable, Text, Text, Text>{
		@Override
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] friendUsers = line.split("\t");
			String friend = friendUsers[0];
			String[] users = friendUsers[1].split(",");
			Arrays.sort(users); 

			
			for(int i=0;i<users.length-1;i++){
				for(int j=i+1;j<users.length;j++){
					context.write(new Text("(["+users[i]+","+users[j]+"],"), new Text(friend));
				}
			}
		}
	}
	

	static class FindFriendsReducer extends Reducer<Text, Text, Text, Text>{
		@Override
		protected void reduce(Text key, Iterable<Text> values,Context context)
				throws IOException, InterruptedException {
			StringBuffer sb = new StringBuffer();
			Set<String> set = new HashSet<String>();
			for(Text friend : values){
				if(!set.contains(friend.toString()))
					set.add(friend.toString());
			}
			for(String friend : set){
				sb.append(friend.toString()).append(",");
			}
			sb.deleteCharAt(sb.length()-1);
			
			context.write(key, new Text("["+sb.toString()+"])"));
		}
	}
	
	public static void main(String[] args)throws Exception {
		Configuration conf = new Configuration();
		String[] OtherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if(OtherArgs.length<2){
			System.err.println("error");
			System.exit(2);
		}



		Job jobone = Job.getInstance(conf);
		jobone.setJarByClass(CommonFriend.class);
		jobone.setMapperClass(MutualFriendsMapper.class);
		jobone.setReducerClass(MutualFriendsReducer.class);
		
		jobone.setOutputKeyClass(Text.class);
		jobone.setOutputValueClass(Text.class);
		
		Path tempDir = new Path("mutual-tmp-output");
		FileInputFormat.setInputPaths(jobone, new Path(OtherArgs[0]));
		FileOutputFormat.setOutputPath(jobone, tempDir);
		
		jobone.waitForCompletion(true);
		

		Job jobtwo = Job.getInstance(conf);
		jobtwo.setJarByClass(CommonFriend.class);
		jobtwo.setMapperClass(FindFriendsMapper.class);
		jobtwo.setReducerClass(FindFriendsReducer.class);
		
		jobtwo.setOutputKeyClass(Text.class);
		jobtwo.setOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(jobtwo, tempDir);
		FileOutputFormat.setOutputPath(jobtwo, new Path(OtherArgs[1]));
		
		boolean res = jobtwo.waitForCompletion(true);
		
		System.exit(res?0:1);
	}
}
