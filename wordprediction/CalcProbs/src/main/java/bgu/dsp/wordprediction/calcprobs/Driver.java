package bgu.dsp.wordprediction.calcprobs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.util.Arrays;
import java.util.Collections;

public class Driver {

    public static void main(String[] args) throws Exception{
        //First Job - Counts total number of words in a given file
        Configuration singleWordConf = new Configuration();
        Job singleWordsJob = new Job(singleWordConf, "WordCount1Gram");
        singleWordsJob.setJarByClass(WordCount1Gram.class);
        singleWordsJob.setMapperClass(WordCount1Gram.MapperClass.class);
        singleWordsJob.setPartitionerClass(WordCount1Gram.PartitionerClass.class);
        singleWordsJob.setCombinerClass(WordCount1Gram.ReducerClass.class);
        singleWordsJob.setReducerClass(WordCount1Gram.ReducerClass.class);
        singleWordsJob.setMapOutputKeyClass(Text.class);
        singleWordsJob.setMapOutputValueClass(LongWritable.class);
        singleWordsJob.setOutputKeyClass(Text.class);
        singleWordsJob.setOutputValueClass(LongWritable.class);
        singleWordsJob.setInputFormatClass(SequenceFileInputFormat.class);
        singleWordsJob.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.addInputPath(singleWordsJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(singleWordsJob, new Path("/Output/1"));

        //Second Job - Counts total number of pair words
        Configuration pairWordsConf = new Configuration();
        Job pairWordsJob = new Job(pairWordsConf, "WordCount2Gram");
        pairWordsJob.setJarByClass(WordCount2Gram.class);
        pairWordsJob.setMapperClass(WordCount2Gram.MapperClass.class);
        pairWordsJob.setPartitionerClass(WordCount2Gram.PartitionerClass.class);
        pairWordsJob.setCombinerClass(WordCount2Gram.ReducerClass.class);
        pairWordsJob.setReducerClass(WordCount2Gram.ReducerClass.class);
        pairWordsJob.setMapOutputKeyClass(Text.class);
        pairWordsJob.setMapOutputValueClass(IntWritable.class);
        pairWordsJob.setOutputKeyClass(Text.class);
        pairWordsJob.setOutputValueClass(IntWritable.class);
        pairWordsJob.setInputFormatClass(SequenceFileInputFormat.class);
        pairWordsJob.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.addInputPath(pairWordsJob, new Path(args[1]));
        FileOutputFormat.setOutputPath(pairWordsJob, new Path("/Output/2"));

        //Third Job - Counts total number of trigram words
        Configuration tuple3WordsConf = new Configuration();
        Job tuple3WordsJob = new Job(tuple3WordsConf, "WordCount3Gram");
        tuple3WordsJob.setJarByClass(WordCount3Gram.class);
        tuple3WordsJob.setMapperClass(WordCount3Gram.MapperClass.class);
        tuple3WordsJob.setPartitionerClass(WordCount3Gram.PartitionerClass.class);
        tuple3WordsJob.setCombinerClass(WordCount3Gram.ReducerClass.class);
        tuple3WordsJob.setReducerClass(WordCount3Gram.ReducerClass.class);
        tuple3WordsJob.setMapOutputKeyClass(Text.class);
        tuple3WordsJob.setMapOutputValueClass(IntWritable.class);
        tuple3WordsJob.setOutputKeyClass(Text.class);
        tuple3WordsJob.setOutputValueClass(IntWritable.class);
        tuple3WordsJob.setInputFormatClass(SequenceFileInputFormat.class);
        tuple3WordsJob.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.addInputPath(tuple3WordsJob, new Path(args[2]));
        FileOutputFormat.setOutputPath(tuple3WordsJob, new Path("/Output/3"));

        //Join: PairWordsJob with TrigramWordsJob
        Configuration join2With3Conf = new Configuration();
        Job join2With3Job = new Job(join2With3Conf, "Join2With3");
        join2With3Job.setJarByClass(Join2with3.class);
        join2With3Job.setPartitionerClass(Join2with3.PartitionerClass.class);
        join2With3Job.setReducerClass(Join2with3.ReducerClass.class);
        join2With3Job.setMapOutputKeyClass(Text.class);
        join2With3Job.setMapOutputValueClass(Text.class);
        join2With3Job.setOutputKeyClass(Text.class);
        join2With3Job.setOutputValueClass(Text.class);
        join2With3Job.setOutputFormatClass(SequenceFileOutputFormat.class);
        MultipleInputs.addInputPath(join2With3Job, new Path("/Output/2"), SequenceFileInputFormat.class, Join2with3.MapperTwoClass.class);
        MultipleInputs.addInputPath(join2With3Job, new Path("/Output/3"), SequenceFileInputFormat.class, Join2with3.MapperThreeClass.class);
        FileOutputFormat.setOutputPath(join2With3Job, new Path("s3n://wordprediction216493892236/SubTasks/Join2With3"));

        //Join: Calculate Probabilities
        Configuration calculateProbsConf = new Configuration();
        calculateProbsConf.set("1GramOutput", "/Output/1");
        Job calcProbsJob = new Job(calculateProbsConf, "CalculateProbs");
        calcProbsJob.setJarByClass(CalcProb.class);
        calcProbsJob.setPartitionerClass(CalcProb.PartitionerClass.class);
        calcProbsJob.setReducerClass(CalcProb.ReducerClass.class);
        calcProbsJob.setMapOutputKeyClass(Text.class);
        calcProbsJob.setMapOutputValueClass(Text.class);
        calcProbsJob.setOutputKeyClass(Text.class);
        calcProbsJob.setOutputValueClass(DoubleWritable.class);
        calcProbsJob.setOutputFormatClass(SequenceFileOutputFormat.class);
        MultipleInputs.addInputPath(calcProbsJob, new Path("s3n://wordprediction216493892236/SubTasks/Join2With3"), SequenceFileInputFormat.class, CalcProb.MapperJoinClass.class);
        MultipleInputs.addInputPath(calcProbsJob, new Path("/Output/3"), SequenceFileInputFormat.class, CalcProb.MapperThreeClass.class);
        FileOutputFormat.setOutputPath(calcProbsJob, new Path("s3n://wordprediction216493892236/SubTasks/CalcProb"));

        //Join: Sort results and probabilities
        Configuration sortProbsConf = new Configuration();
        Job sortProbsJob = new Job(sortProbsConf, "sortProbs");
        sortProbsJob.setJarByClass(SortProbs.class);
        sortProbsJob.setMapperClass(SortProbs.MapperClass.class);
        sortProbsJob.setPartitionerClass(SortProbs.PartitionerClass.class);
        sortProbsJob.setReducerClass(SortProbs.ReducerClass.class);
        sortProbsJob.setMapOutputKeyClass(Text.class);
        sortProbsJob.setMapOutputValueClass(Text.class);
        sortProbsJob.setOutputKeyClass(Text.class);
        sortProbsJob.setOutputValueClass(DoubleWritable.class);
        sortProbsJob.setInputFormatClass(SequenceFileInputFormat.class);
        FileInputFormat.addInputPath(sortProbsJob, new Path("s3n://wordprediction216493892236/SubTasks/CalcProb"));
        FileOutputFormat.setOutputPath(sortProbsJob, new Path(args[3]));

        //Job control
        ControlledJob singleWordsJC = new ControlledJob(singleWordsJob, Collections.<ControlledJob>emptyList());
        ControlledJob pairWordsJC = new ControlledJob(pairWordsJob, Collections.<ControlledJob>emptyList());
        ControlledJob tuple3WordsJC = new ControlledJob(tuple3WordsJob, Collections.<ControlledJob>emptyList());
        ControlledJob join2With3JC = new ControlledJob(join2With3Job, Arrays.asList(pairWordsJC,tuple3WordsJC));
        ControlledJob calcProbJC = new ControlledJob(calcProbsJob, Arrays.asList(singleWordsJC,tuple3WordsJC,join2With3JC));
        ControlledJob sortProbsJC = new ControlledJob(sortProbsJob, Arrays.asList(calcProbJC));
        JobControl jobControl = new JobControl("wordPrediction");
        jobControl.addJobCollection(Arrays.asList(singleWordsJC,pairWordsJC,tuple3WordsJC,join2With3JC,calcProbJC,sortProbsJC));
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
