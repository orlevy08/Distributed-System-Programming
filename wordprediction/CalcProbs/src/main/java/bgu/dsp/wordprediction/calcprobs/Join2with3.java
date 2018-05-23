package bgu.dsp.wordprediction.calcprobs;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Join2with3 {

    private final static String wildCard = "*";

    public static class MapperTwoClass extends Mapper<Text, IntWritable, Text, Text> {

        @Override
        public void map(Text key, IntWritable value, Context context) throws IOException,  InterruptedException {
            String newKey = key.toString().replaceAll("[^א-ת ]", "");
            String [] words = newKey.split(" ");
            if(words.length == 2 && !words[0].equals("") && !words[1].equals(""))
                context.write(new Text(newKey), new Text(value.toString()));
        }
    }

    public static class MapperThreeClass extends Mapper<Text, IntWritable, Text, Text> {

        @Override
        public void map(Text key, IntWritable value, Context context) throws IOException,  InterruptedException {
            String newKey = key.toString().replaceAll("[^א-ת ]", "");
            String [] words = newKey.split(" ");
            if(words.length == 3 && !words[0].equals("") && !words[1].equals("") && !words[2].equals("")) {
                context.write(new Text(words[0] + " " + words[1] + " " + wildCard), new Text(newKey));
                context.write(new Text(words[1] + " " + words[2] + " " + wildCard), new Text(newKey));
            }
        }
    }

    public static class ReducerClass extends Reducer<Text,Text,Text,Text> {

        private int currNumOfOccurrences;

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
            if (key.toString().contains(wildCard)) {
                Text newValue = new Text (key.toString()
                        .replace(" "+wildCard, "")
                        + ":" + currNumOfOccurrences);
                for (Text value : values) {
                    context.write(value, newValue);
                }
            }
            else {
                for (Text value : values)
                    currNumOfOccurrences = Integer.parseInt(value.toString());
            }
        }
    }

    public static class PartitionerClass extends Partitioner<Text, Text> {

        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            Text newKey = new Text(key.toString().replace(" "+wildCard, ""));
            return Math.abs(newKey.hashCode()) % numPartitions;
        }
    }
}
