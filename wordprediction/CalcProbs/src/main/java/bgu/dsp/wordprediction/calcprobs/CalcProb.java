package bgu.dsp.wordprediction.calcprobs;

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
import java.util.HashMap;
import java.util.Map;


public class CalcProb {

    private static final String wildCard = "*";

    public static class MapperJoinClass extends Mapper<Text, Text, Text, Text> {

        @Override
        public void map(Text key, Text value, Context context) throws IOException,  InterruptedException {
            context.write(key, value);
        }
    }

    public static class MapperThreeClass extends Mapper<Text, IntWritable, Text, Text> {

        @Override
        public void map(Text key, IntWritable value, Context context) throws IOException,  InterruptedException {
            String newKey = key.toString().replaceAll("[^א-ת ]", "");
            String [] words = newKey.split(" ");
            if(words.length == 3 && !words[0].equals("") && !words[1].equals("") && !words[2].equals(""))
                context.write(new Text(newKey), new Text(value.toString()));
        }
    }

    public static class ReducerClass extends Reducer<Text,Text,Text,DoubleWritable> {

        Map<String, Long> singleWordsOccurrences = new HashMap();

        @Override
        public void setup(Context context) {
            Configuration conf = context.getConfiguration();
            Path dir = new Path(conf.get("1GramOutput"));
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
                        singleWordsOccurrences.put(key.toString(), ((LongWritable)value).get());

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
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,InterruptedException {
            double n3 = 0, c2 = 0, n2 = 0;
            String [] words = key.toString().split(" ");
            for (Text value : values){
                if (value.toString().contains(words[0]+" "+words[1]))
                    c2 = Double.parseDouble(value.toString().split(":")[1]);
                else if (value.toString().contains(words[1]+" "+words[2]))
                    n2 = Double.parseDouble(value.toString().split(":")[1]);
                else
                    n3 = Double.parseDouble(value.toString());
            }
            double c0 = singleWordsOccurrences.get(wildCard);
            double c1 = singleWordsOccurrences.get(words[1]);
            double n1 = singleWordsOccurrences.get(words[2]);

            double log2 = Math.log10(n2+1);
            double k2 = (log2+1) / (log2+2);
            double log3 = Math.log10(n3+1);
            double k3 = (log3+1) / (log3+2);

            double prob = (k3*(n3/c2)) + ((1-k3)*k2*(n2/c1)) + ((1-k3)*(1-k2)*(n1/c0));
            prob = Double.valueOf(String.format("%.3f", prob));
            context.write(key, new DoubleWritable(prob));
        }
    }

    public static class PartitionerClass extends Partitioner<Text, Text> {

        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            return Math.abs(key.hashCode()) % numPartitions;
        }
    }

}
