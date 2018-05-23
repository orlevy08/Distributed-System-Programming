package bgu.dsp.semanticclassification.mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class ConcatenatePatterns {

    public static class MapperClass extends Mapper<Text, Text, Text, Text> {

        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            context.write(key, value);
        }
    }

    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder str = new StringBuilder();
            Set<String> patterns = new HashSet<>();
            for (Text value : values) {
                if (!patterns.contains(value.toString())) {
                    str.append(value.toString() + ";");
                    patterns.add(value.toString());
                }
            }
            str.setLength(str.length()-1);
            context.write(key, new Text(str.toString()));
        }
    }
}
