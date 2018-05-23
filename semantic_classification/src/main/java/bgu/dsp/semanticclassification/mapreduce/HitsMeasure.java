package bgu.dsp.semanticclassification.mapreduce;

import bgu.dsp.semanticclassification.models.Tuple2;
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

public class HitsMeasure {

    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {

        Set<MapredCluster> clusters = new HashSet<>();
        Map<Tuple2<String,String>, String> dataSet = new HashMap<>();
        double alpha;
        int counter = 0;

        @Override
        public void setup(Mapper.Context context) {
            Configuration conf = context.getConfiguration();
            alpha = Double.valueOf(conf.get("alpha"));
            Path clustersDir = new Path(conf.get("clustersFile"));
            Path dataSetDir = new Path(conf.get("dataSetFile"));
            FileSystem hdfs = null;
            SequenceFile.Reader reader = null;
            try {
                hdfs = FileSystem.get(conf);
                FileStatus[] clustersFileStatuses = hdfs.listStatus(clustersDir);
                for (int i = 0; i < clustersFileStatuses.length; i++) {
                    Path path = clustersFileStatuses[i].getPath();
                    if(path.getName().contains("_SUCCESS"))
                        continue;
                    reader = new SequenceFile.Reader(hdfs, path, conf);
                    Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
                    Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
                    while (reader.next(key, value)) {
                        String[] splits = value.toString().split(";");
                        List<String> core = (!splits[0].isEmpty())
                                ? Arrays.asList(splits[0].split(","))
                                : Collections.emptyList();
                        List<String> unconfirmed = (splits.length == 2)
                                ? Arrays.asList(splits[1].split(","))
                                : Collections.emptyList();
                        MapredCluster cluster = MapredCluster.builder()
                                .core(core)
                                .unconfirmed(unconfirmed)
                                .build();
                        clusters.add(cluster);
                    }
                }
                FileStatus[] dataSetFileStatuses = hdfs.listStatus(dataSetDir);
                for (int i = 0; i < dataSetFileStatuses.length; i++) {
                    Path path = dataSetFileStatuses[i].getPath();
                    if(path.getName().contains("_SUCCESS"))
                        continue;
                    reader = new SequenceFile.Reader(hdfs, path, conf);
                    Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
                    Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
                    while (reader.next(key, value)) {
                        String[] keySplits = key.toString().split(",");
                        Tuple2<String,String> entryKey = Tuple2.of(keySplits[0],keySplits[1]);
                        dataSet.put(entryKey, value.toString());
                    }
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
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if(counter == 0) {
                String[] splits = value.toString().split("\t");
                String ngram = splits[0];
                String[] words = ngram.split(" ");
                if (words.length == 5) {
                    Text newKey;
                    Tuple2<String, String> temp;
                    if (dataSet.containsKey((temp = Tuple2.of(words[1], words[3]))))
                        newKey = new Text(words[1] + "," + words[3] + ":" + dataSet.get(temp));
                    else if (dataSet.containsKey((temp = Tuple2.of(words[3], words[1]))))
                        newKey = new Text(words[3] + "," + words[1] + ":" + dataSet.get(temp));
                    else
                        return;
                    String corpusPattern = words[0] + "_" + words[2] + "_" + words[4];
                    StringBuilder vector = new StringBuilder();
                    for (MapredCluster cluster : clusters) {
                        if (cluster.getCore().contains(corpusPattern)) {
                            vector.append((1f) / cluster.numOfCore() + ",");
                        } else if (cluster.getUnconfirmed().contains(corpusPattern)) {
                            vector.append((alpha) / cluster.numOfUnconfirmed() + ",");
                        } else {
                            vector.append(0 + ",");
                        }
                    }
                    vector.setLength(vector.length() - 1);
                    Text vectorVal = new Text(vector.toString());
                    context.write(newKey, vectorVal);
                }
            }
            counter = ++counter % 5;
        }
    }

    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double[] vector = null;
            for (Text value : values) {
                String[] part = value.toString().split(",");
                if(vector == null)
                    vector = new double[part.length];
                for (int i = 0; i < part.length; i++)
                    vector[i] += Double.parseDouble(part[i]);
            }
            StringBuilder str = new StringBuilder();
            for (int i = 0; i < vector.length; i++)
                str.append(vector[i]+",");
            str.setLength(str.length()-1);
            context.write(key, new Text(str.toString()));
        }
    }
}
