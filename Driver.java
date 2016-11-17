package complete;

import java.io.BufferedWriter;

import java.io.File;
import java.io.FileWriter;
import org.apache.hadoop.io.IntWritable.Comparator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;
/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;;

public class Driver {

	static Random rand = new Random();
	static char[] ltable = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '+', '-', '*', '/' };
	static int chromoLen = 0;
	static int poolSize = 0;

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		chromoLen = Integer.parseInt(args[0]);
		poolSize = Integer.parseInt(args[1]);
		String fileName = "chromosome.txt";
		build(fileName);

		Configuration conf = new Configuration();
		conf.set("chromoLen", args[0]);
		conf.set("poolSize", args[1]);
		conf.set("crossRate", args[2]);
		conf.set("mutRate", args[3]);
		conf.set("target", args[4]);
		conf.set("result_path", args[5]);

		// job.setMapOutputKeyClass(Text.class);
		// job.setMapOutputValueClass(Text.class);

		Path inPath = new Path(fileName);
		Path outPath = null;

		int i = 0;
		while (i < 500) {		

			Job job = new Job(conf);

			job.setJarByClass(Driver.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			job.setMapperClass(IterMap.class);
			job.setReducerClass(IterReduce.class);

			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);

			outPath = new Path(args[6] + i);
			FileInputFormat.addInputPath(job, inPath);
			FileOutputFormat.setOutputPath(job, outPath);
			job.waitForCompletion(true);
			if (new File(args[5]).exists()) {
				break;
			}
			inPath = outPath;
			i++;
			
			
		}
		
		if(i==500){
			
			Job job = new Job(conf);
			job.setSortComparatorClass(LongWritable.DecreasingComparator.class);
			//job.setSortComparatorClass(LongWritable.Sorter.class);
			job.setJarByClass(Driver.class);
			job.setMapOutputKeyClass(DoubleWritable.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			job.setMapperClass(Fitness.class);
			job.setReducerClass(Reducer.class);
			
			//job.setReducerClass(InverseReducer.class);

			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			FileInputFormat.addInputPath(job, outPath);
			FileOutputFormat.setOutputPath(job, new Path("close_shot"));
			job.waitForCompletion(true);
		}

	}

	public static void build(String fileName) throws IOException {

		// Must be even
		StringBuffer chromo;

		// Assume default encoding.
		FileWriter fileWriter = new FileWriter(fileName);

		// Always wrap FileWriter in BufferedWriter.
		BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);

		for (int i = 0; i < poolSize; i++) {
			chromo = new StringBuffer(chromoLen * 4);
			for (int y = 0; y < chromoLen; y++) {
				// What's the current length
				int pos = chromo.length();

				// Generate a random binary integer
				String binString = Integer.toBinaryString(rand.nextInt(ltable.length));
				int fillLen = 4 - binString.length();

				// Fill to 4
				for (int x = 0; x < fillLen; x++)
					chromo.append('0');

				// Append the chromo
				chromo.append(binString);

			}
			bufferedWriter.write(chromo.toString());
			bufferedWriter.newLine();

		}

		bufferedWriter.close();

	}
	
//	public class Sorter extends Comparator {
//
//		 protected Sorter() {
//		        super(Text.class, true);
//		    }
//
//		    @SuppressWarnings("rawtypes")
//		    @Override
//		    public int compare(WritableComparable w1, WritableComparable w2) {
//		        LongWritable key1 = (LongWritable) w1;
//		        LongWritable key2 = (LongWritable) w2;          
//		        return -1 * key1.compareTo(key2);
//		    }
//
//	    
//	}

}
