package bgu.dsp.semanticclassification.mapreduce;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class JoinPatternsPMI {

    public static class PatternsMapperClass extends Mapper<Text, Text, Text, Text> {

        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            context.write(key, new Text("Patterns:"+value.toString()));
        }
    }

    public static class PMIMapperClass extends Mapper<Text, DoubleWritable, Text, Text> {

        @Override
        public void map(Text key, DoubleWritable value, Context context) throws IOException, InterruptedException {
            context.write(key, new Text("PMI:"+value.get()));
        }
    }

    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String patterns = null;
            String pmi = null;
            for (Text value : values) {
                String[] splits = value.toString().split(":");
                if(splits[0].equals("Patterns"))
                    patterns = splits[1];
                else if(splits[0].equals("PMI"))
                    pmi = splits[1];
            }
            if(patterns != null){
                if(pmi == null)
                    pmi = String.format("%f",Double.MIN_VALUE);
                context.write(key, new Text(pmi+"$"+patterns));
            }
        }
    }
}
