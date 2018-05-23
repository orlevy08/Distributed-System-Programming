package bgu.dsp.semanticclassification.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class PatternIdentification {

    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {

        Set<String> hfw = new HashSet<>();
        Set<String> hooks = new HashSet<>();
        Set<String> targets = new HashSet<>();
        int counter = 0;

        @Override
        public void setup(Mapper.Context context) {
            Configuration conf = context.getConfiguration();
            Path hfwDir = new Path(conf.get("hfwFile"));
            Path hooksDir = new Path(conf.get("hooksFile"));
            Path targetsDir = new Path(conf.get("targetsFile"));
            FileSystem hdfs = null;
            SequenceFile.Reader reader = null;
            BufferedReader br = null;
            try {
                hdfs = FileSystem.get(conf);
                FileStatus[] hfwFileStatuses = hdfs.listStatus(hfwDir);
                FileStatus[] hooksFileStatuses = hdfs.listStatus(hooksDir);
                FileStatus[] targetsFileStatuses = hdfs.listStatus(targetsDir);
                for (int i = 0; i < hfwFileStatuses.length; i++) {
                    Path path = hfwFileStatuses[i].getPath();
                    if(path.getName().contains("_SUCCESS"))
                        continue;
                    reader = new SequenceFile.Reader(hdfs, path, conf);
                    Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
                    Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
                    while (reader.next(key, value)) {
                        hfw.add(key.toString());
                    }
                }
                for (int i = 0; i < hooksFileStatuses.length; i++) {
                    Path path = hooksFileStatuses[i].getPath();
                    if(path.getName().contains("_SUCCESS"))
                        continue;
                    reader = new SequenceFile.Reader(hdfs, path, conf);
                    Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
                    Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
                    while (reader.next(key, value)) {
                        hooks.add(key.toString());
                    }
                }
                for (int i = 0; i < targetsFileStatuses.length; i++) {
                    Path path = targetsFileStatuses[i].getPath();
                    if(path.getName().contains("_SUCCESS"))
                        continue;
                    br = new BufferedReader(new InputStreamReader(hdfs.open(path)));
                    String line;
                    while ((line = br.readLine()) != null)
                        targets.add(line.split("\t")[0]);
                }
            } catch(IOException e){
                e.printStackTrace();
                System.exit(-1);
            }finally{
                IOUtils.closeStream(br);
                IOUtils.closeStream(reader);
                IOUtils.closeStream(hdfs);
            }
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if(counter == 0) {
                String ngram = value.toString().split("\t")[0];
                String[] words = ngram.split(" ");
                if (words.length == 5) {
                    if (hfw.containsAll(Arrays.asList(words[0], words[2], words[4]))) {
                        if (hooks.contains(words[1]) && targets.contains(words[3])) {
                            Text newKey = new Text(words[1] + "," + words[3]);
                            Text newValue = new Text(words[0] + "_" + words[2] + "_" + words[4]);
                            context.write(newKey, newValue);
                        }
                        if (hooks.contains(words[3]) && targets.contains(words[1])) {
                            Text newKey = new Text(words[3] + "," + words[1]);
                            Text newValue = new Text(words[0] + "_" + words[2] + "_" + words[4]);
                            context.write(newKey, newValue);
                        }
                    }
                }
            }
            counter = ++counter % 5;
        }
    }

    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values)
                context.write(key, value);
        }
    }
}
