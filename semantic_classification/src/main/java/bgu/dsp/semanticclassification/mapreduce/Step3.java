package bgu.dsp.semanticclassification.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.util.Arrays;
import java.util.Collections;

public class Step3 {
    public static void main(String[] args) throws Exception {

        //Hooks file
        Configuration conf1 = new Configuration();
        Job job1 = new Job(conf1, "Hooks");
        job1.setJarByClass(WriteHooks.class);
        job1.setMapperClass(WriteHooks.MapperClass.class);
        job1.setReducerClass(WriteHooks.ReducerClass.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        //Pattern Identification
        Configuration conf2 = new Configuration();
        conf2.set("hooksFile", args[1]);
        conf2.set("hfwFile", args[2]);
        conf2.set("targetsFile", args[3]);
        Job job2 = new Job(conf2, "PatternIdentification");
        job2.setJarByClass(PatternIdentification.class);
        job2.setMapperClass(PatternIdentification.MapperClass.class);
        job2.setReducerClass(PatternIdentification.ReducerClass.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        job2.setInputFormatClass(SequenceFileInputFormat.class);
        job2.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.addInputPath(job2, new Path(args[4]));
        FileOutputFormat.setOutputPath(job2, new Path(args[5]));

        //Filter patterns
        Configuration conf3 = new Configuration();
        Job job3 = new Job(conf3, "FilterPatterns");
        job3.setJarByClass(FilterPatterns.class);
        job3.setMapperClass(FilterPatterns.MapperClass.class);
        job3.setReducerClass(FilterPatterns.ReducerClass.class);
        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(Text.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);
        job3.setInputFormatClass(SequenceFileInputFormat.class);
        job3.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.addInputPath(job3, new Path(args[5]));
        FileOutputFormat.setOutputPath(job3, new Path(args[6]));

        //Concatenate patterns
        Configuration conf4 = new Configuration();
        Job job4 = new Job(conf4, "ConcatenatePatterns");
        job4.setJarByClass(ConcatenatePatterns.class);
        job4.setMapperClass(ConcatenatePatterns.MapperClass.class);
        job4.setReducerClass(ConcatenatePatterns.ReducerClass.class);
        job4.setMapOutputKeyClass(Text.class);
        job4.setMapOutputValueClass(Text.class);
        job4.setOutputKeyClass(Text.class);
        job4.setOutputValueClass(Text.class);
        job4.setInputFormatClass(SequenceFileInputFormat.class);
        job4.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.addInputPath(job4, new Path(args[5]));
        FileOutputFormat.setOutputPath(job4, new Path(args[7]));

        //CalcPMI
        Configuration conf5 = new Configuration();
        conf5.set("singleWords", args[10]);
        Job job5 = new Job(conf5, "CalcPMI");
        job5.setJarByClass(CalcPMI.class);
        job5.setMapperClass(CalcPMI.MapperClass.class);
        job5.setReducerClass(CalcPMI.ReducerClass.class);
        job5.setMapOutputKeyClass(Text.class);
        job5.setMapOutputValueClass(LongWritable.class);
        job5.setOutputKeyClass(Text.class);
        job5.setOutputValueClass(DoubleWritable.class);
        job5.setInputFormatClass(SequenceFileInputFormat.class);
        job5.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.addInputPath(job5, new Path(args[8]));
        FileOutputFormat.setOutputPath(job5, new Path(args[9]));

        //JoinPatternsAndPMI
        Configuration conf6 = new Configuration();
        Job job6 = new Job(conf6, "JoinPatternsAndPMI");
        job6.setJarByClass(JoinPatternsPMI.class);
        job6.setReducerClass(JoinPatternsPMI.ReducerClass.class);
        job6.setMapOutputKeyClass(Text.class);
        job6.setMapOutputValueClass(Text.class);
        job6.setOutputKeyClass(Text.class);
        job6.setOutputValueClass(Text.class);
        MultipleInputs.addInputPath(job6, new Path(args[7]), SequenceFileInputFormat.class, JoinPatternsPMI.PatternsMapperClass.class);
        MultipleInputs.addInputPath(job6, new Path(args[9]), SequenceFileInputFormat.class, JoinPatternsPMI.PMIMapperClass.class);
        FileOutputFormat.setOutputPath(job6, new Path(args[11]));

        //Job control
        ControlledJob jc1 = new ControlledJob(job1, Collections.<ControlledJob>emptyList());
        ControlledJob jc2 = new ControlledJob(job2, Collections.<ControlledJob>singletonList(jc1));
        ControlledJob jc3 = new ControlledJob(job3, Collections.<ControlledJob>singletonList(jc2));
        ControlledJob jc4 = new ControlledJob(job4, Collections.<ControlledJob>singletonList(jc3));
        ControlledJob jc5 = new ControlledJob(job5, Collections.<ControlledJob>emptyList());
        ControlledJob jc6 = new ControlledJob(job6, Arrays.asList(jc4,jc5));
        JobControl jobControl = new JobControl("ProcessPatternsWithPMI");
        jobControl.addJobCollection(Arrays.asList(jc1,jc2,jc3,jc4,jc5,jc6));
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
