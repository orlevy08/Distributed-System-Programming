package bgu.dsp.semanticclassification.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;
import java.util.*;

public class CalcPMI {

    public static class MapperClass extends Mapper<Text, LongWritable, Text, LongWritable> {

        @Override
        public void map(Text key, LongWritable value, Context context) throws IOException, InterruptedException {
            String[] words = key.toString().split(" ");
            if(words.length == 2) {
                Text newKey = new Text(words[0] + "," + words[1]);
                context.write(newKey, value);
            }
        }
    }

    public static class ReducerClass extends Reducer<Text, LongWritable, Text, DoubleWritable> {
        Map<String, Long> singleWords = new HashMap<>();

        @Override
        public void setup(Reducer.Context context) {
            Configuration conf = context.getConfiguration();
            Path dir = new Path(conf.get("singleWords"));
            FileSystem hdfs = null;
            SequenceFile.Reader reader = null;
            try {
                hdfs = FileSystem.get(conf);
                FileStatus[] fileStatuses = hdfs.listStatus(dir);
                for (int i = 0; i < fileStatuses.length; i++) {
                    Path path = fileStatuses[i].getPath();
                    if(path.getName().contains("_SUCCESS"))
                        continue;
                    reader = new SequenceFile.Reader(hdfs, path, conf);
                    Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
                    Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
                    while (reader.next(key, value))
                        singleWords.put(key.toString(), ((LongWritable)value).get());
                }
            } catch(IOException e){
                e.printStackTrace();
                System.exit(-1);
            }finally{
                IOUtils.closeStream(reader);
                IOUtils.closeStream(hdfs);
            }
        }

        @Override
        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            String[] words = key.toString().split(",");
            double pmi = 0.0;
            for (LongWritable value : values) {
                pmi += Math.log((double)value.get());
            }
            pmi += Math.log(singleWords.get("*"));
            pmi -= Math.log(singleWords.get(words[0]));
            pmi -= Math.log(singleWords.get(words[1]));
            context.write(key, new DoubleWritable(pmi));
        }
    }
}
