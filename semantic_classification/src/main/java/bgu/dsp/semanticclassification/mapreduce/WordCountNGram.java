package bgu.dsp.semanticclassification.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.regex.Pattern;

public class WordCountNGram {

    public static class MapperClass extends Mapper<LongWritable, Text, Text, LongWritable> {

        private final static Text wildCard = new Text("*");


        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] row = value.toString().split("\t");
            String ngram = row[0];
            Pattern pattern = Pattern.compile("^[A-Za-z' ]+$");
            if (pattern.matcher(ngram).matches()) {
                LongWritable count = new LongWritable(Long.valueOf(row[2]));
                context.write(new Text(ngram), count);
                context.write(wildCard, count);
            }
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
