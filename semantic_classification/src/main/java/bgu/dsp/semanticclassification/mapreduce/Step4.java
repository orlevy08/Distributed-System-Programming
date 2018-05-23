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

public class Step4 {

    public static void main(String[] args) throws Exception {

        //Write Clusters
        Configuration conf1 = new Configuration();
        Job job1 = new Job(conf1, "WriteClusters");
        job1.setJarByClass(WriteClusters.class);
        job1.setMapperClass(WriteClusters.MapperClass.class);
        job1.setReducerClass(WriteClusters.ReducerClass.class);
        job1.setMapOutputKeyClass(LongWritable.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(LongWritable.class);
        job1.setOutputValueClass(Text.class);
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1,new Path(args[1]));

        //Write DataSet
        Configuration conf2 = new Configuration();
        Job job2 = new Job(conf2, "WriteDataSet");
        job2.setJarByClass(WriteDataSet.class);
        job2.setMapperClass(WriteDataSet.MapperClass.class);
        job2.setReducerClass(WriteDataSet.ReducerClass.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        job2.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.addInputPath(job2, new Path(args[2]));
        FileOutputFormat.setOutputPath(job2,new Path(args[3]));

        //Hits Measure
        Configuration conf3 = new Configuration();
        conf3.set("clustersFile", args[1]);
        conf3.set("dataSetFile", args[3]);
        conf3.set("alpha", args[6]);
        Job job3 = new Job(conf3, "HitsMeasure");
        job3.setJarByClass(HitsMeasure.class);
        job3.setMapperClass(HitsMeasure.MapperClass.class);
        job3.setCombinerClass(HitsMeasure.ReducerClass.class);
        job3.setReducerClass(HitsMeasure.ReducerClass.class);
        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(Text.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);
        job3.setInputFormatClass(SequenceFileInputFormat.class);
        FileInputFormat.addInputPath(job3, new Path(args[4]));
        FileOutputFormat.setOutputPath(job3, new Path(args[5]));

        //Job control
        ControlledJob jc1 = new ControlledJob(job1, Collections.<ControlledJob>emptyList());
        ControlledJob jc2 = new ControlledJob(job2, Collections.<ControlledJob>emptyList());
        ControlledJob jc3 = new ControlledJob(job3, Arrays.asList(jc1,jc2));
        JobControl jobControl = new JobControl("HitsMeasure");
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
