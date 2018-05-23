package bgu.dsp.semanticclassification;

import bgu.dsp.semanticclassification.models.Cluster;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.*;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.transfer.MultipleFileDownload;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import javafx.util.Pair;

import java.io.*;
import java.util.*;
import java.util.stream.Collectors;

public class Main {
    public static void main(String[] args) throws Exception{

        String clusterId = "j-ZO0EG5CL8OJ1";
        AmazonElasticMapReduce mapReduce = AmazonElasticMapReduceClientBuilder.defaultClient();
        AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();
        TransferManager transferManager = TransferManagerBuilder.defaultTransferManager();


        double fh = 10f/1000000;
        double fc = 5000f/1000000;

        HadoopJarStepConfig hadoopJarStep1 = new HadoopJarStepConfig()
                .withJar("s3n://semantic-classification-dsp-181/step-1.jar")
                .withArgs("s3n://semantic-classification-dsp-181/dataset.txt",
                        "s3n://semantic-classification-dsp-181/Output/WordCountDataSet");
        StepConfig stepConfig1 = new StepConfig()
                .withName("WordCount")
                .withHadoopJarStep(hadoopJarStep1)
                .withActionOnFailure("CONTINUE");
        AddJobFlowStepsResult result1 = mapReduce.addJobFlowSteps(new AddJobFlowStepsRequest()
                .withJobFlowId(clusterId)
                .withSteps(stepConfig1));
        String step1Id = result1.getStepIds().get(0);

        //Copying targets to hdfs
        HadoopJarStepConfig hadoopJarStepTargets = new HadoopJarStepConfig()
                .withJar("command-runner.jar")
                .withArgs("s3-dist-cp","--s3Endpoint=s3.amazonaws.com",
                        "--src","s3n://semantic-classification-dsp-181/Output/WordCountDataSet",
                        "--dest", "hdfs:///Output/1/Targets");
        StepConfig stepConfigTargets = new StepConfig()
                .withName("CopyTargetsToHDFS")
                .withHadoopJarStep(hadoopJarStepTargets)
                .withActionOnFailure("CONTINUE");
        mapReduce.addJobFlowSteps(new AddJobFlowStepsRequest()
                .withJobFlowId(clusterId)
                .withSteps(stepConfigTargets));

        HadoopJarStepConfig hadoopJarStep2 = new HadoopJarStepConfig()
                .withJar("s3n://semantic-classification-dsp-181/step-2.jar")
                .withArgs("s3n://datasets.elasticmapreduce/ngrams/books/20090715/eng-us-all/1gram/data",
                        "/Output/1/WordCount1Gram",
                        "s3n://datasets.elasticmapreduce/ngrams/books/20090715/eng-us-all/2gram/data",
                        "s3n://semantic-classification-dsp-181/Output/WordCount2Gram",
                        "/Output/1/HFW",
                        String.format("%f",fh),
                        String.format("%f",fc));
                StepConfig stepConfig2 = new StepConfig()
                .withName("ExtractWords")
                .withHadoopJarStep(hadoopJarStep2)
                .withActionOnFailure("CONTINUE");
        mapReduce.addJobFlowSteps(new AddJobFlowStepsRequest()
                .withJobFlowId(clusterId)
                .withSteps(stepConfig2));

        wait_step_1_complete:
        while(true) {
            StepState state = StepState.valueOf(mapReduce.describeStep(new DescribeStepRequest()
                            .withStepId(step1Id)
                            .withClusterId(clusterId))
                    .getStep()
                    .getStatus()
                    .getState());
            switch (state) {
                case FAILED: System.exit(-1);
                case COMPLETED: break wait_step_1_complete;
                default: Thread.sleep(30000);
            }
        }

        Map<String, Long> allWords = new HashMap<>();
        File dir = new File("WordCountDataSet");
        MultipleFileDownload download =  transferManager.downloadDirectory("semantic-classification-dsp-181", "Output/WordCountDataSet", dir);
        download.waitForCompletion();
        dir = new File(dir.getPath()+"/Output/WordCountDataSet");
        for (File file : dir.listFiles()) {
            try(BufferedReader br = new BufferedReader(new FileReader(file))) {
                String line;
                while ((line = br.readLine()) != null) {
                    String[] splits = line.split("\t");
                    allWords.put(splits[0], Long.valueOf(splits[1]));
                }
            }
        }

        //Extract Hooks
        String filename = "hooks.txt";
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(filename))) {
            allWords.entrySet().stream()
                    .limit(1500)
                    .forEach(entry -> {
                        if (!entry.getKey().equals("*")) {
                            try {
                                bw.write(entry.getKey() + "\n");
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }
                    });
        }

        //Upload hooks file to S3
        s3.putObject(new PutObjectRequest(
            "semantic-classification-dsp-181",
            filename,
            new File(filename)));

        HadoopJarStepConfig hadoopJarStep3 = new HadoopJarStepConfig()
                .withJar("s3n://semantic-classification-dsp-181/step-3.jar")
                .withArgs("s3n://semantic-classification-dsp-181/hooks.txt",
                        "/Output/1/Hooks",
                        "/Output/1/HFW",
                        "/Output/1/Targets",
                        "s3n://datasets.elasticmapreduce/ngrams/books/20090715/eng-us-all/5gram/data",
                        "s3n://semantic-classification-dsp-181/Output/PatternIdentification",
                        "s3n://semantic-classification-dsp-181/Output/FilterPatterns",
                        "s3n://semantic-classification-dsp-181/Output/ConcatenatePatterns",
                        "s3n://semantic-classification-dsp-181/Output/WordCount2Gram",
                        "s3n://semantic-classification-dsp-181/Output/CalcPMI",
                        "/Output/1/WordCount1Gram",
                        "s3n://semantic-classification-dsp-181/Output/JoinPatternsAndPMI");
        StepConfig stepConfig3 = new StepConfig()
                .withName("ProcessPatterns")
                .withHadoopJarStep(hadoopJarStep3)
                .withActionOnFailure("CONTINUE");
        AddJobFlowStepsResult result3 = mapReduce.addJobFlowSteps(new AddJobFlowStepsRequest()
                .withJobFlowId(clusterId)
                .withSteps(stepConfig3));
        String step3Id = result3.getStepIds().get(0);


        wait_step_3_complete:
        while(true) {
            StepState state = StepState.valueOf(mapReduce.describeStep(new DescribeStepRequest()
                    .withStepId(step3Id)
                    .withClusterId(clusterId))
                    .getStep()
                    .getStatus()
                    .getState());
            switch (state) {
                case FAILED: System.exit(-1);
                case COMPLETED: break wait_step_3_complete;
                default: Thread.sleep(30000);
            }
        }

        //phases 1,2 of the algorithm are complete
        Map<String, Map<String, Pair<Double, Cluster>>> clustersPrePMI = new HashMap<>();
        dir = new File("Patterns");
        download =  transferManager.downloadDirectory("semantic-classification-dsp-181", "Output/JoinPatternsAndPMI", dir);
        download.waitForCompletion();
        dir = new File(dir.getPath()+"/Output/JoinPatternsAndPMI");
        for (File file : dir.listFiles()) {
            try(BufferedReader br = new BufferedReader(new FileReader(file))) {
                String line;
                while ((line = br.readLine()) != null) {
                    String[] keyValue = line.split("\t");
                    String[] hookTarget = keyValue[0].split(",");
                    String hook = hookTarget[0];
                    String target = hookTarget[1];
                    String[] pmiPattern = keyValue[1].split("\\$");
                    double pmi = Double.valueOf(pmiPattern[0]);
                    String[] rawPatterns = pmiPattern[1].split(";");
                    Set<Pair<String, String>> patterns = Arrays.stream(rawPatterns)
                            .map(str -> new Pair<>(str, "unconfirmed"))
                            .collect(Collectors.toSet());
                    if (!clustersPrePMI.containsKey(hook))
                        clustersPrePMI.put(hook, new HashMap<>());
                    Cluster cluster = Cluster.builder()
                            .cluster(patterns)
                            .allUnconfirmed(true)
                            .numOfPatterns(patterns.size())
                            .numOfCore(0)
                            .numOfUnconfirmed(patterns.size())
                            .build();
                    clustersPrePMI.get(hook).put(target, new Pair<>(pmi, cluster));
                }
            }
        }

        //PMI filtering
        double l = 0.2;
        Map<String, Map<String, Cluster>> clusters = filterByPMI(clustersPrePMI, l);

        double s = 2f/3;
        //Algorithm - merge clusters of the hook corpora step
        for(Map.Entry entry : clusters.entrySet()){
            Map<String, Cluster> hookClusters = clusters.get(entry.getKey());
            for(String target : new HashSet<>(hookClusters.keySet())) {
                Cluster c1 = hookClusters.get(target);
                if (c1 != null){
                    Iterator<Map.Entry<String, Cluster>> it = hookClusters.entrySet().iterator();
                    while (it.hasNext()) {
                        Map.Entry<String, Cluster> otherTargetCluster = it.next();
                        Cluster c2 = otherTargetCluster.getValue();
                        if (!(c1 == c2)) {
                            Set<String> shared = new HashSet<>(c2.getAllPatterns());
                            shared.retainAll(c1.getAllPatterns());
                            if (c2.getCore().equals(c1.getCore()) &&
                                    shared.size() >= (s * c2.getNumOfPatterns())) {
                                c1.merge(c2);
                                it.remove();
                            }
                        }
                    }
                }
            }
        }

        //Algorithm Step 3
        Pair<String,String> hookTarget;
        while((hookTarget = getMinOfAllUnconfirmed(clusters)) != null) {
            Cluster c1 = clusters.get(hookTarget.getKey()).get(hookTarget.getValue());
            Set<String> otherHooks = new HashSet<>(clusters.keySet());
            otherHooks.remove(hookTarget.getKey());
            for (String hook : otherHooks) {
                Map<String, Cluster> hookClusters = clusters.get(hook);
                Iterator<Map.Entry<String,Cluster>> it = hookClusters.entrySet().iterator();
                while (it.hasNext()) {
                    Map.Entry<String,Cluster> entry = it.next();
                    Cluster c2 = entry.getValue();
                    Set<String> shared = new HashSet<>(c2.getAllPatterns());
                    shared.retainAll(c1.getAllPatterns());
                    if (c2.getCore().equals(c1.getCore()) &&
                            shared.size() >= (s*c2.getNumOfPatterns())) {
                        c1.merge(c2);
                        it.remove();
                    }
                }
            }
            if(c1.isAllUnconfirmed())
                clusters.get(hookTarget.getKey()).remove(hookTarget.getValue());
        }

        //Algorithm Step 4
        for (String currHook : clusters.keySet()) {
            for (String currTarget : clusters.get(currHook).keySet()) {
                Cluster currCluster = clusters.get(currHook).get(currTarget);
                Set<String> otherHooks = new HashSet<>(clusters.keySet());
                otherHooks.remove(currHook);
                for (String otherHook : otherHooks) {
                    Map<String, Cluster> otherHookClusters = clusters.get(otherHook);
                    Iterator<Map.Entry<String,Cluster>> it = otherHookClusters.entrySet().iterator();
                    while (it.hasNext()) {
                        Map.Entry<String,Cluster> entry = it.next();
                        Cluster otherCluster = entry.getValue();
                        Set<String> shared = new HashSet<>(otherCluster.getAllPatterns());
                        shared.retainAll(currCluster.getAllPatterns());
                        if (otherCluster.getCore().equals(currCluster.getCore()) &&
                                shared.size() >= (s*otherCluster.getNumOfPatterns())) {
                            currCluster.merge(otherCluster);
                            it.remove();
                        }
                    }
                }
            }
        }

        //Writing clusters to a file
        filename = "clusters.txt";
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(filename))) {
            clusters.entrySet()
                    .stream()
                    .map(Map.Entry::getValue)
                    .flatMap(map->
                        map.entrySet().stream().map(Map.Entry::getValue))
                    .forEach(cluster -> {
                        try {
                            StringBuilder str = new StringBuilder();
                            for (String core : cluster.getCore()) {
                                str.append(core + ",");
                            }
                            if(cluster.getNumOfCore() > 0)
                                str.setLength(str.length()-1);
                            str.append(';');
                            for (String unconfirmed : cluster.getUnconfirmed()) {
                                str.append(unconfirmed + ",");
                            }
                            if(cluster.getNumOfUnconfirmed() > 0)
                                str.setLength(str.length()-1);
                            bw.write(str.toString() + "\n");
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    });
        }

        //Upload clusters file to S3
        s3.putObject(new PutObjectRequest(
            "semantic-classification-dsp-181",
            filename,
            new File(filename)));

        //Hits Measure
        double alpha = 0.1;
        HadoopJarStepConfig hadoopJarStep4 = new HadoopJarStepConfig()
                .withJar("s3n://semantic-classification-dsp-181/step-4.jar")
                .withArgs("s3n://semantic-classification-dsp-181/clusters.txt",
                        "/Output/2/Clusters",
                        "s3n://semantic-classification-dsp-181/dataset.txt",
                        "/Output/2/DataSet",
                        "s3n://datasets.elasticmapreduce/ngrams/books/20090715/eng-us-all/5gram/data",
                        "s3n://semantic-classification-dsp-181/Output/HitsMeasure",
                        String.format("%f",alpha));
        StepConfig stepConfig4 = new StepConfig()
                .withName("HitsMeasure")
                .withHadoopJarStep(hadoopJarStep4)
                .withActionOnFailure("CONTINUE");
        AddJobFlowStepsResult result4 = mapReduce.addJobFlowSteps(new AddJobFlowStepsRequest()
                .withJobFlowId(clusterId)
                .withSteps(stepConfig4));
        String step4Id = result4.getStepIds().get(0);

        wait_step_4_complete:
        while(true) {
            StepState state = StepState.valueOf(mapReduce.describeStep(new DescribeStepRequest()
                    .withStepId(step4Id)
                    .withClusterId(clusterId))
                    .getStep()
                    .getStatus()
                    .getState());
            switch (state) {
                case FAILED: System.exit(-1);
                case COMPLETED: break wait_step_4_complete;
                default: Thread.sleep(30000);
            }
        }

        dir = new File("HitsMeasure");
        filename = "newDataSet.csv";
        download =  transferManager.downloadDirectory("semantic-classification-dsp-181", "Output/HitsMeasure", dir);
        download.waitForCompletion();
        transferManager.shutdownNow();
        dir = new File(dir.getPath()+"/Output/HitsMeasure");
        for (File file : dir.listFiles()) {
            try (BufferedReader br = new BufferedReader(new FileReader(file));
                BufferedWriter bw = new BufferedWriter(new FileWriter(filename))) {
                String line;
                while ((line = br.readLine()) != null) {
                    String[] splits = line.split("\t");
                    String label = splits[0].split(":")[1];
                    String vector = splits[1];
                    if(vector.matches(".*[1-9]+.*"))
                        bw.write(vector + "," + label + "\n");
                }
            }
        }
    }

    private static Pair<String,String> getMinOfAllUnconfirmed(Map<String, Map<String, Cluster>> clusters) {
        return clusters.entrySet().stream()
                .flatMap(entry -> {
                    Set<String> targets = entry.getValue().keySet();
                    return targets.stream()
                            .map(target -> new Pair<>(entry.getKey(), target));
                })
                .filter(pair -> clusters.get(pair.getKey()).get(pair.getValue()).isAllUnconfirmed())
                .min(Comparator.comparing(pair -> clusters.get(pair.getKey()).get(pair.getValue()).getNumOfPatterns()))
                .orElse(null);
    }

    private static Map<String, Map<String, Cluster>> filterByPMI(Map<String, Map<String, Pair<Double,Cluster>>> clustersPrePMI, double l){
        return clustersPrePMI.entrySet().parallelStream()
                .map(hookEntry -> {
                    int size = hookEntry.getValue().size();
                    int numToRemove = (int)(l*size);
                    Map<String,Cluster> newVal = hookEntry.getValue().entrySet().stream()
                            .sorted(Comparator.comparing(targetEntry -> targetEntry.getValue().getKey()))
                            .skip(numToRemove)
                            .limit(size - 2*numToRemove)
                            .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().getValue()));
                    return new Pair<>(hookEntry.getKey(), newVal);
                })
                .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
    }
}
