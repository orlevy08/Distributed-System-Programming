package bgu.dsp.semanticclassification.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Step1 {

    public static void main(String[] args) throws Exception {

        //Data Set Word Count
        Configuration conf = new Configuration();
        Job job = new Job(conf, "WordCountDataSet");
        job.setJarByClass(WordCountDataSet.class);
        job.setMapperClass(WordCountDataSet.MapperClass.class);
        job.setCombinerClass(WordCountDataSet.ReducerClass.class);
        job.setReducerClass(WordCountDataSet.ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
