package bgu.dsp.semanticclassification.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.regex.Pattern;

public class WordCountDataSet {

    public static class MapperClass extends Mapper<LongWritable, Text, Text, LongWritable> {

        private final static Text wildCard = new Text("*");
        private final static LongWritable one = new LongWritable(1L);


        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] row = value.toString().split("\t");
            String w1 = row[0];
            String w2 = row[1];
            context.write(new Text(w1), one);
            context.write(new Text(w2), one);
            context.write(wildCard, one);
        }
    }

    public static class ReducerClass extends Reducer<Text, LongWritable, Text, LongWritable> {

        @Override
        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable value : values) {
                sum += value.get();
            }
            context.write(key, new LongWritable(sum));
        }
    }
}

