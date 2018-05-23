package bgu.dsp.sarcasm.manager;

import bgu.dsp.sarcasm.common.Review;
import bgu.dsp.sarcasm.common.Reviews;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.*;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class Manager {

    private static final AmazonS3 S3 = AmazonS3ClientBuilder.defaultClient();
    private static final AmazonEC2 EC2 = AmazonEC2ClientBuilder.defaultClient();
    private static final AmazonSQS SQS = AmazonSQSClientBuilder.defaultClient();
    private static final String INPUT_QUEUE_URL = SQS.getQueueUrl("inputQueue").getQueueUrl();
    private static final String TASKS_QUEUE_URL = SQS.getQueueUrl("tasksQueue").getQueueUrl();
    private static final String RESULTS_QUEUE_URL = SQS.getQueueUrl("resultsQueue").getQueueUrl();
    private static final String OUTPUT_QUEUE_URL = SQS.getQueueUrl("outputQueue").getQueueUrl();
    private static final String BUCKET_NAME = "sarcasm216493892236";
    private static final String WORKER_ROLE_ARN = "arn:aws:iam::216493892236:instance-profile/SarcasmWorkerRole";
    private static final String WORKER_IMAGE_ID = "ami-1582126f";
    private static final ExecutorService executors = Executors.newFixedThreadPool(2);
    private static final SyncFileManager fileManager = new SyncFileManager();
    private static final ObjectMapper mapper = new ObjectMapper();


    public static void main(String[] args) throws Exception {

        AtomicBoolean shouldTerminate = new AtomicBoolean(false);
        AtomicInteger ratio = new AtomicInteger(Integer.MAX_VALUE);
        List<String> workerIds = new ArrayList<>();

        // --------------------------------------------
        // THREAD 1: Handling Results and terminates
        // --------------------------------------------
        Thread resultListener = new Thread(() -> {

            ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest()
                    .withQueueUrl(RESULTS_QUEUE_URL)
                    .withMessageAttributeNames("filename");

            while (!(fileManager.isEmpty() && shouldTerminate.get())) {
                List<Message> resultMessages = SQS.receiveMessage((receiveMessageRequest)).getMessages();
                for (Message resultMessage : resultMessages) {
                    SQS.deleteMessage(RESULTS_QUEUE_URL, resultMessage.getReceiptHandle());
                    submitResult(resultMessage);
                }
            }

            //TERMINATE
            EC2.terminateInstances(new TerminateInstancesRequest()
                    .withInstanceIds(workerIds));
            executors.shutdownNow();
            //selfTerminate();

        });
        resultListener.start();

        // --------------------------------------------
        // THREAD 2: Initiates workers as necessary
        // --------------------------------------------
        Thread workerInitiator = new Thread(() -> {

            int previousNumOfTasks = 0, currentNumOfTasks = 0;
            while(!(shouldTerminate.get() && (previousNumOfTasks > currentNumOfTasks))) {
                previousNumOfTasks = currentNumOfTasks;
                currentNumOfTasks = Integer.valueOf(SQS.getQueueAttributes(new GetQueueAttributesRequest()
                        .withQueueUrl(TASKS_QUEUE_URL)
                        .withAttributeNames(QueueAttributeName.ApproximateNumberOfMessages))
                        .getAttributes()
                        .get(QueueAttributeName.ApproximateNumberOfMessages.toString()));
                int numOfRequiredWorkers = currentNumOfTasks / ratio.get();
                if (workerIds.size() < numOfRequiredWorkers) {
                    RunInstancesRequest runInstancesRequest = new RunInstancesRequest()
                            .withImageId(WORKER_IMAGE_ID)
                            .withIamInstanceProfile(new IamInstanceProfileSpecification()
                                    .withArn(WORKER_ROLE_ARN))
                            .withMinCount(1)
                            .withMaxCount(numOfRequiredWorkers - workerIds.size())
                            .withInstanceType(InstanceType.T2Medium.toString());
                    workerIds.addAll(
                            EC2.runInstances(runInstancesRequest)
                                    .getReservation().getInstances()
                                    .stream()
                                    .map(Instance::getInstanceId)
                                    .collect(Collectors.toList()));
                }
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        });
        workerInitiator.start();

        // ------------------------------------------
        // MAIN THREAD: Handling Inputs from LocalApp
        // ------------------------------------------
        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest()
                .withQueueUrl(INPUT_QUEUE_URL)
                .withMessageAttributeNames("workersToTasksRatio");
        boolean terminated = false;

        while(!shouldTerminate.get()) {
            List<Message> taskMessages = SQS.receiveMessage((receiveMessageRequest)).getMessages();
            for (Message taskMessage : taskMessages) {
                //Deletes handled message from SQS queue
                SQS.deleteMessage(INPUT_QUEUE_URL, taskMessage.getReceiptHandle());
                if(taskMessage.getBody().equals("terminate")) {
                    terminated = true;
                    continue;
                }
                String filename = taskMessage.getBody();
                fileManager.addFile(filename);
                System.out.println(String.format("handling file: '%s'...", filename));
                System.out.println(String.format(
                        "total number of files received so far is: %d", fileManager.getTotalNumOfFilesReceived()));
                submitTask(taskMessage);
                String workersToTasksRatio = taskMessage.getMessageAttributes()
                        .get("workersToTasksRatio").getStringValue();
                ratio.set(Math.min(ratio.get(), Integer.valueOf(workersToTasksRatio)));
            }
            if (terminated)
                System.out.println("received 'termination' message");
            shouldTerminate.set(terminated);
        }
        workerInitiator.join();
        resultListener.join();
    }

    /*
     * This function submit the following task to the executors service:
     * Receives a message from input queue - containing the filename
     * Downloads the file from s3
     * Parse the file to basic reviews
     * Sends each review to task queue (workers)
     */
    private static void submitTask(Message message){
        executors.execute(() -> {
            String filename = message.getBody();
            //Downloads the file from S3
            S3Object object = S3.getObject(
                    new GetObjectRequest(BUCKET_NAME, "Input/" + filename));
            InputStream inputStream = object.getObjectContent();
            List<String> allLines = null;
            try {
                allLines = IOUtils.readLines(inputStream, "UTF-8");
                inputStream.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            Map<String, MessageAttributeValue> taskAttributes = new HashMap<>();
            taskAttributes.put("filename" , new MessageAttributeValue()
                    .withDataType("String")
                    .withStringValue(filename));
            List<Reviews> allReviews = allLines.stream()
                    .map(json -> {
                        try {
                            return mapper.readValue(json, Reviews.class);
                        }catch (IOException e){
                            e.printStackTrace();
                        }
                        return null;
                    })
                    .collect(Collectors.toList());
            for (Reviews reviews: allReviews) {
                for (Review review : reviews.getReviews()) {
                    executors.execute(() -> {
                        fileManager.incrementNumOfTasks(filename);
                        try {
                            SQS.sendMessage(new SendMessageRequest()
                                    .withQueueUrl(TASKS_QUEUE_URL)
                                    .withMessageBody(mapper.writeValueAsString(review))
                                    .withMessageAttributes(taskAttributes));
                        } catch (IOException e) {
                            fileManager.decrementNumOfTasks(filename);
                            e.printStackTrace();
                        }
                    });
                }
            }
        });
    }

    /*
    * This function submit the following task to the executors service:
    * Receives a message from results queue sent from workers
    * Writes the result to output file and when finished - uploads it to s3
    * Sends the message to output queue (localApp)
    */
    private static void submitResult(Message message){
        executors.execute(() -> {
            String filename = message.getMessageAttributes()
                    .get("filename").getStringValue();
            String result = message.getBody();
            try {
                if (fileManager.writeToFile(filename, result)) {
                    //Uploads a file to S3 and send message
                    File file = new File(filename);
                    PutObjectRequest req = new PutObjectRequest(
                            BUCKET_NAME, "Output/" + filename, file);
                    S3.putObject(req);
                    file.delete();
                    SQS.sendMessage(new SendMessageRequest()
                            .withQueueUrl(OUTPUT_QUEUE_URL)
                            .withMessageBody(filename));
                    System.out.println(String.format("successfully handled file '%s'", filename));
                    System.out.println(String.format(
                            "number of files handled so far is: %d", fileManager.getTotalNumOfFilesSent()));
                }
            }catch (Exception e){
                e.printStackTrace();
            }
        });
    }

    /*
     * This function self terminates the manager instance
     */
    private static void selfTerminate(){
        DescribeInstancesRequest getInstanceRequest = new DescribeInstancesRequest()
                .withFilters(new Filter()
                        .withName("tag:isManager")
                        .withValues("true"));
        String managerId = EC2.describeInstances(getInstanceRequest)
                .getReservations().get(0).getInstances().get(0).getInstanceId();
        EC2.terminateInstances(new TerminateInstancesRequest()
                .withInstanceIds(managerId));
    }

}
