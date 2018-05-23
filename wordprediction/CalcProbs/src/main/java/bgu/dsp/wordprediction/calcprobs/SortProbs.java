package bgu.dsp.wordprediction.calcprobs;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SortProbs {

    public static class MapperClass extends Mapper<Text, DoubleWritable, Text, Text> {

        @Override
        public void map(Text key, DoubleWritable value, Context context) throws IOException,  InterruptedException {
            String[] words = key.toString().split(" ");
            context.write(new Text(words[0] + " " + words[1] + " "
                    + (1-value.get())),new Text(key + "\t" + value));
        }
    }

    public static class ReducerClass extends Reducer<Text,Text,Text,DoubleWritable> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
            for (Text value : values){
                String[] keyValue = value.toString().split("\t");
                context.write(new Text(keyValue[0]),
                        new DoubleWritable(Double.parseDouble(keyValue[1])));
            }
        }
    }

    public static class PartitionerClass extends Partitioner<Text, Text> {

        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            String[] words = key.toString().split(" ");
            String newKey = (words[0] + " " + words[1]);
            return Math.abs(newKey.hashCode()) % numPartitions;
        }
    }
}
