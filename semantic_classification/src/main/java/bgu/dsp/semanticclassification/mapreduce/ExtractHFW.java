package bgu.dsp.semanticclassification.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;

public class ExtractHFW {

    public static class MapperClass extends Mapper<Text, LongWritable, Text, LongWritable> {

        @Override
        public void map(Text key, LongWritable value, Context context) throws IOException, InterruptedException {
            if (key.toString().equals("*")) {
                int numOfReducers = context.getNumReduceTasks();
                for (int i = 0; i < numOfReducers; i++) {
                    context.write(new Text("*"+i), value);
                }
            }
            else
                context.write(key, value);
        }
    }

    public static class ReducerClass extends Reducer<Text, LongWritable, Text, LongWritable> {

        long numOfTotalOccurrences;
        double fh;
        double fc;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            fh = Double.valueOf(conf.get("fh"));
            fc = Double.valueOf(conf.get("fc"));
        }

        @Override
        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            for (LongWritable value : values) {
                if (key.toString().contains("*"))
                    numOfTotalOccurrences = value.get();

                else if (((double)value.get())/numOfTotalOccurrences >= fh &&
                        ((double)value.get())/numOfTotalOccurrences <= fc)
                    context.write(key, value);
            }
        }
    }

    public static class PartitionerClass extends Partitioner<Text, LongWritable> {

        @Override
        public int getPartition(Text key, LongWritable value, int numOfPartitions) {
            if (key.toString().contains("*"))
                return Integer.parseInt(key.toString().substring(1));
            else
                return Math.abs(key.hashCode()) % numOfPartitions;
        }
    }
}
