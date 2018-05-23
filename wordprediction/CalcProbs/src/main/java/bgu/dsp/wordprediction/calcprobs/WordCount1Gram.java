package bgu.dsp.wordprediction.calcprobs;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordCount1Gram {

    public static class MapperClass extends Mapper<LongWritable, Text, Text, LongWritable> {

        private final static Text wildCard = new Text("*");

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
            String[] row =  value.toString().split("\t");
            String ngram = row[0];
            String count = row[2];
            LongWritable newVal = new LongWritable(Long.valueOf(count));
            context.write(new Text(ngram), newVal);
            context.write(wildCard, newVal);
        }
    }

    public static class ReducerClass extends Reducer<Text,LongWritable,Text,LongWritable> {

        @Override
        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException,  InterruptedException {
            long sum = 0;
            for (LongWritable value : values) {
                sum += value.get();
            }
            context.write(key, new LongWritable(sum));
        }
    }

    public static class PartitionerClass extends Partitioner<Text, LongWritable> {

        @Override
        public int getPartition(Text key, LongWritable value, int numPartitions) {
            return Math.abs(key.hashCode()) % numPartitions;
        }
    }
}
