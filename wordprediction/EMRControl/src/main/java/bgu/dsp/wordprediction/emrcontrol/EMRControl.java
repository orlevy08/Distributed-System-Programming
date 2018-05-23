package bgu.dsp.wordprediction.emrcontrol;

import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.*;

public class EMRControl {

    public static void main(String[] args) {

        AmazonElasticMapReduce mapReduce = AmazonElasticMapReduceClientBuilder.defaultClient();

        HadoopJarStepConfig hadoopJarStep = new HadoopJarStepConfig()
                .withJar("s3n://wordprediction216493892236/calc-probs-1.0-SNAPSHOT-jar-with-dependencies.jar") // This should be a full map reduce application.
                .withArgs("s3n://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/1gram/data",
                        "s3n://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/2gram/data",
                        "s3n://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/3gram/data",
                        args[1]);

        StepConfig stepConfig = new StepConfig()
                .withName("Word Prediction")
                .withHadoopJarStep(hadoopJarStep)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        AddJobFlowStepsResult result = mapReduce.addJobFlowSteps(new AddJobFlowStepsRequest()
                .withJobFlowId(args[0])
                .withSteps(stepConfig));

        String stepId = result.getStepIds().get(0);
        System.out.println("sent job: "+ stepId);
    }
}
