package bgu.dsp.semanticclassification.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.util.Arrays;
import java.util.Collections;

public class Step2 {

    public static void main(String[] args) throws Exception {

        //Word Count 1 Gram
        Configuration conf1 = new Configuration();
        Job job1 = new Job(conf1, "WordCountNGram");
        job1.setJarByClass(WordCountNGram.class);
        job1.setMapperClass(WordCountNGram.MapperClass.class);
        job1.setCombinerClass(WordCountNGram.ReducerClass.class);
        job1.setReducerClass(WordCountNGram.ReducerClass.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(LongWritable.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(LongWritable.class);
        job1.setInputFormatClass(SequenceFileInputFormat.class);
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        //WordCount2Gram
        Configuration conf2 = new Configuration();
        Job job2 = new Job(conf2, "WordCount2Gram");
        job2.setJarByClass(WordCountNGram.class);
        job2.setMapperClass(WordCountNGram.MapperClass.class);
        job2.setCombinerClass(WordCountNGram.ReducerClass.class);
        job2.setReducerClass(WordCountNGram.ReducerClass.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(LongWritable.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(LongWritable.class);
        job2.setInputFormatClass(SequenceFileInputFormat.class);
        job2.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.addInputPath(job2, new Path(args[2]));
        FileOutputFormat.setOutputPath(job2, new Path(args[3]));

        //Extract HFW
        Configuration conf3 = new Configuration();
        conf3.set("fh", args[5]);
        conf3.set("fc", args[6]);
        Job job3 = new Job(conf3, "ExtractHFW");
        job3.setJarByClass(ExtractHFW.class);
        job3.setMapperClass(ExtractHFW.MapperClass.class);
        job3.setReducerClass(ExtractHFW.ReducerClass.class);
        job3.setPartitionerClass(ExtractHFW.PartitionerClass.class);
        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(LongWritable.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(LongWritable.class);
        job3.setInputFormatClass(SequenceFileInputFormat.class);
        job3.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.addInputPath(job3, new Path(args[1]));
        FileOutputFormat.setOutputPath(job3, new Path(args[4]));

//        //Extract Hooks
//        Configuration conf3 = new Configuration();
//        conf3.set("fb", args[6]);
//        conf3.set("fc", args[4]);
//        Job job3 = new Job(conf3, "ExtractHooks");
//        job3.setJarByClass(ExtractHooks.class);
//        job3.setMapperClass(ExtractHooks.MapperClass.class);
//        job3.setReducerClass(ExtractHooks.ReducerClass.class);
//        job3.setPartitionerClass(ExtractHooks.PartitionerClass.class);
//        job3.setMapOutputKeyClass(Text.class);
//        job3.setMapOutputValueClass(LongWritable.class);
//        job3.setOutputKeyClass(Text.class);
//        job3.setOutputValueClass(LongWritable.class);
//        job3.setInputFormatClass(SequenceFileInputFormat.class);
//        FileInputFormat.addInputPath(job3, new Path(args[1]));
//        FileOutputFormat.setOutputPath(job3, new Path(args[3]));

        //Job control
        ControlledJob jc1 = new ControlledJob(job1, Collections.<ControlledJob>emptyList());
        ControlledJob jc2 = new ControlledJob(job2, Collections.<ControlledJob>emptyList());
        ControlledJob jc3 = new ControlledJob(job3, Collections.<ControlledJob>singletonList(jc1));
        JobControl jobControl = new JobControl("ExtractWords");
        jobControl.addJobCollection(Arrays.asList(jc1,jc2,jc3));
        Thread jobControlThread = new Thread(jobControl);
        jobControlThread.start();

        while (!jobControl.allFinished()) {
            if(jobControl.getFailedJobList().size() > 0)
                System.exit(-1);
            Thread.sleep(30000);
        }
        System.exit(0);
    }
}
