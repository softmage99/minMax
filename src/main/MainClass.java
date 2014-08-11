package main;

import java.io.IOException;

import map.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import reduce.Reduce;



public class MainClass {

	/**
	 * @param args
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		// TODO Auto-generated method stub
		
		/*JobConf conf = new JobConf(MainClass.class);
		 conf.setJobName("sample");
		 
		 //conf.setMapperClass();
		 //conf.setReducerClass();
		 
		 conf.setInputFormat(TextInputFormat.class);
		 FileInputFormat.setInputPaths(conf, args[0]);
		 
		 conf.setMapOutputKeyClass(Text.class);
		 conf.setMapOutputKeyClass(Text.class);
		 
		 conf.setOutputKeyClass(Text.class);
		 conf.setOutputValueClass(vo.class);
		 
		 conf.setOutputFormat(TextOutputFormat.class);
		 FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		 
		 JobClient.runJob(conf);*/
		
		
		Configuration conf = new Configuration();
		 
		 System.out.println("1");
		 @SuppressWarnings("deprecation")
		Job job = new Job(conf, "MainClass");
		 job.setJarByClass(MainClass.class);
		 
		 System.out.println("2");
		 
		 job.setMapperClass(Map.class);
		 job.setCombinerClass(Reduce.class);
		 job.setReducerClass(Reduce.class);
		 
		 System.out.println("3");
		 
		 job.setInputFormatClass(TextInputFormat.class);
		 TextInputFormat.setInputPaths(job, args[0]);
		 
		 System.out.println("4");
		 
		 job.setOutputFormatClass(TextOutputFormat.class);
		 TextOutputFormat.setOutputPath(job, new Path(args[1]));
		 
		 System.out.println("5");
		 
		 job.setMapOutputKeyClass(Text.class);
		 job.setMapOutputValueClass(vo.class);
		 
		 System.out.println("6");
		 
		 job.setOutputKeyClass(Text.class);
		 job.setOutputValueClass(vo.class);
		 
		 System.out.println("7");
		 
		 System.exit(job.waitForCompletion(true) ? 0 : 1);
		 
		 System.out.println("8");
		 
	}

}
