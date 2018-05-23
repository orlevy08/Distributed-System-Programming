package bgu.dsp.sarcasm;

import com.amazonaws.AmazonServiceException;
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
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;

import java.io.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static j2html.TagCreator.*;


public class LocalApp {

    private static final String ID = UUID.randomUUID().toString();
    private static final AmazonS3 S3 = AmazonS3ClientBuilder.defaultClient();
    private static final AmazonEC2 EC2 = AmazonEC2ClientBuilder.defaultClient();
    private static final AmazonSQS SQS = AmazonSQSClientBuilder.defaultClient();
    private static final String INPUT_QUEUE_URL = SQS.getQueueUrl("inputQueue").getQueueUrl();
    private static final String OUTPUT_QUEUE_URL = SQS.getQueueUrl("outputQueue").getQueueUrl();
    private static final String BUCKET_NAME = "sarcasm216493892236";
    private static final String MANAGER_ROLE_ARN = "arn:aws:iam::216493892236:instance-profile/SarcasmManagerRole";
    private static final String MANAGER_IMAGE_ID = "ami-bdba2ac7";


    public static void main(String[] args) throws Exception {

        try {

            long startTime = System.currentTimeMillis();

            if (!managerExists())
                createManager();

            int numOfInputFiles = args.length - 1;
            String workersToTasksRatio = args[args.length-1];
            if(args[args.length-1].equals("terminate")) {
                numOfInputFiles--;
                workersToTasksRatio = args[args.length-2];
            }
            //Creating message attributes
            Map<String, MessageAttributeValue> attributes = new HashMap<>();
            attributes.put("workersToTasksRatio", new MessageAttributeValue()
                    .withDataType("String")
                    .withStringValue(workersToTasksRatio));

            //Uploads a file to S3 and sends message
            for (int i = 0; i < numOfInputFiles; i++) {
                File file = new File(args[i]);
                String filename = ID + "/" + args[i];
                uploadFile(file, filename);
                sendMessage(filename, attributes);
                System.out.println(String.format("uploaded file #%d: '%s'", i+1, filename));
            }

            if (args[args.length-1].equals("terminate")) {
                SendMessageRequest terminationMessageRequest = new SendMessageRequest()
                        .withQueueUrl(INPUT_QUEUE_URL)
                        .withMessageBody("terminate")
                        .withDelaySeconds(60);
                SQS.sendMessage(terminationMessageRequest);
                System.out.println("sent 'termination' message");
            }

            System.out.println("waiting for results...");
            int numOfOutputFiles = 0;
            ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest()
                    .withQueueUrl(OUTPUT_QUEUE_URL);
            while (numOfOutputFiles < numOfInputFiles) {
                List<Message> outputMessages = SQS.receiveMessage(receiveMessageRequest).getMessages();
                for (Message outputMessage: outputMessages) {
                    String filename = outputMessage.getBody();
                    if (filename.contains(ID)) {
                        SQS.deleteMessage(OUTPUT_QUEUE_URL, outputMessage.getReceiptHandle());
                        numOfOutputFiles++;
                        System.out.println(String.format("received result #%d: '%s'", numOfOutputFiles, filename));
                        //Downloads a file from S3
                        S3Object object = S3.getObject(
                                new GetObjectRequest(BUCKET_NAME, "Output/" + filename));
                        InputStream inputStream = object.getObjectContent();
                        List<String> allReviews = IOUtils.readLines(inputStream, "UTF-8");
                        inputStream.close();

                        System.out.println("generating html file...");
                        generateHtml(filename, allReviews);
                        double elapsedTimeSeconds = ((double)(System.currentTimeMillis()-startTime))/1000;
                        System.out.println(String.format(
                                "finished processing file '%s' in %.3f seconds", filename, elapsedTimeSeconds));
                    }
                }
            }

        } catch (AmazonServiceException ase) {
            System.out.println("Caught Exception: " + ase.getMessage());
            System.out.println("Response Status Code: " + ase.getStatusCode());
            System.out.println("Error Code: " + ase.getErrorCode());
            System.out.println("Request ID: " + ase.getRequestId());
        }
    }


    private static boolean managerExists() {
        DescribeInstancesRequest getInstanceRequest = new DescribeInstancesRequest()
                .withFilters(new Filter()
                        .withName("tag:isManager")
                        .withValues("true"));
        List<Reservation> reservations = EC2.describeInstances(getInstanceRequest)
                .getReservations();
        return !reservations.isEmpty();
    }

    private static void createManager() {
        System.out.println("creating a 'Manager' instance...");
        RunInstancesRequest runInstancesRequest = new RunInstancesRequest()
                .withImageId(MANAGER_IMAGE_ID)
                .withMinCount(1)
                .withMaxCount(1)
                .withIamInstanceProfile(new IamInstanceProfileSpecification()
                        .withArn(MANAGER_ROLE_ARN))
                .withInstanceType(InstanceType.T2Micro.toString());

        List<Instance> instances = EC2.runInstances(runInstancesRequest).getReservation().getInstances();
        EC2.createTags(new CreateTagsRequest()
                .withResources(instances.get(0).getInstanceId())
                .withTags(new Tag()
                        .withKey("isManager")
                        .withValue("true")));
    }

    private static void uploadFile(File file, String filename) {
        PutObjectRequest req = new PutObjectRequest(
                BUCKET_NAME, "Input/" + filename, file);
        S3.putObject(req);
    }

    private static void sendMessage(String message,
                    Map<String, MessageAttributeValue> attributes) {
        SQS.sendMessage(new SendMessageRequest()
                .withQueueUrl(INPUT_QUEUE_URL)
                .withMessageBody(message) //send message to queue
                .withMessageAttributes(attributes));
    }

    private static void generateHtml(String filename, List<String> allReviews) {
        List<String[]> allParsedReviwes = allReviews.stream()
                .map(str -> str.split("\t"))
                .collect(Collectors.toList());

        String html =

        html(
          head(
             title(filename)
          ),
          body(
              div(attrs("#Reviews"),
                  each(allParsedReviwes, review->
                      div(attrs(".Review"),
                          tag("font").attr("color",getColor(review[7])).with(
                              h3(review[0]),//Title
                              p(review[1]),//Review
                              p(review[2]),//Rating
                              p(review[3]),//Author
                              p(join("Link: ",
                                      a().withHref(review[4].replace("Link: ", ""))
                                         .withText(review[4].replace("Link: ", "")))),//Link
                              p(review[5]),//Entities
                              p(review[6]),//Sarcastic
                              p(review[7]),//Sentiment
                              br()
                          )
                      )
                  )
              )
          )
        ).render();

        File outputFile = new File (FilenameUtils.removeExtension(filename) + ".html");
        File directory = new File(filename.split("/")[0]);
        directory.mkdir();
        try (PrintWriter writer = new PrintWriter(outputFile)) {
            writer.print(html);
            writer.flush();
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    private static String getColor(String sentiment){
      sentiment = sentiment.replace("Sentiment: ", "");
      int sentimentValue = Integer.valueOf(sentiment);
      switch(sentimentValue) {
          case 0: return "darkred";
          case 1: return "red";
          case 2: return "black";
          case 3: return "#00cc66"; //green
          case 4: return "#006600"; //darkgreen
      }
      return null;
    }
}
