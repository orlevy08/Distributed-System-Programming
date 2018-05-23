package bgu.dsp.semanticclassification.mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FilterPatterns {

    public static class MapperClass extends Mapper<Text, Text, Text, Text> {

        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            context.write(value, key);
        }
    }

    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String hook = null;
            boolean test = false;
            for (Text value : values) {
                if (hook == null)
                    hook = value.toString().split(",")[0];
                else if (!value.toString().split(",")[0].equals(hook)) {
                    test = true;
                    break;
                }
            }
            if(test) {
                for (Text value : values)
                    context.write(value, key);
            }
        }
    }
}
