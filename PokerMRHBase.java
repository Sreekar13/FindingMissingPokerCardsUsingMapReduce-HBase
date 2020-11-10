import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;

public class PokerMRHBase {
	static int id=0,rows=0;
	//Class for Mapper
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable>{
		public void map(LongWritable value, Text line, Context out) throws IOException, InterruptedException {
			String l=line.toString();
			String[] lineArray=l.split(",");
			Text suit=new Text(lineArray[0]);
			IntWritable rank=new IntWritable(Integer.parseInt(lineArray[1]));
			out.write(suit, rank);
		}
	}	
	
	//Class for Reducer
	public static class Reduce extends TableReducer<Text, IntWritable, ImmutableBytesWritable>{
		public void reduce(Text suit, Iterable<IntWritable> rank, Context out) throws IOException, InterruptedException {
			List<Integer> deckSuit=new ArrayList<Integer>();
			for(int i=1;i<=13;i++) {
				deckSuit.add(i);
			}
			List<Integer> actualSuit=new ArrayList<Integer>();
			for(IntWritable i :rank) {
				actualSuit.add(i.get());
			}
			List<Integer> missingSuit=new ArrayList<Integer>(deckSuit);
			missingSuit.removeAll(actualSuit);
			 
			for(Integer missing: missingSuit) {
				System.out.println("- cards"+"-"+suit+"-"+Integer.toString(missing));
				Put put=new Put(("row"+Integer.toString(id)).getBytes());
				id++;rows++;
				put.addColumn("cards".getBytes(),Bytes.toBytes(suit.toString()),Integer.toString(missing).getBytes());
				out.write(null,put);
			}				
		}
	}
	
	
	
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf =  HBaseConfiguration.create();
		Job job = new Job(conf,"PokerMRHBase");
		job.setJarByClass(PokerMRHBase.class);
		job.setMapperClass(Map.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		TableMapReduceUtil.initTableReducerJob("poker",Reduce.class, job);
		job.setReducerClass(Reduce.class);
	    job.waitForCompletion(true);
	    System.out.println("==================================== Total number of missing cards: "+rows+" ====================================");
	}
	
	
}
