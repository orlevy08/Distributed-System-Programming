package bgu.dsp.sarcasm.worker;

import bgu.dsp.sarcasm.common.Review;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreAnnotations.NamedEntityTagAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TextAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;


public class Worker {

    private static final AmazonSQS SQS = AmazonSQSClientBuilder.defaultClient();
    private static final String TASKS_QUEUE_URL = SQS.getQueueUrl("tasksQueue").getQueueUrl();
    private static final String RESULTS_QUEUE_URL = SQS.getQueueUrl("resultsQueue").getQueueUrl();
    private static final ObjectMapper mapper = new ObjectMapper();


    public static void main(String[] args) throws Exception {

        ReceiveMessageRequest receiveMessageRequest =
                new ReceiveMessageRequest()
                        .withQueueUrl(TASKS_QUEUE_URL)
                        .withMessageAttributeNames("filename")
                        .withMaxNumberOfMessages(1);
        int numOfTasksHandeled = 0;
        while(true) {
            try {
                List<Message> message = SQS.receiveMessage((receiveMessageRequest)).getMessages(); //Blocking
                if (!message.isEmpty()) {

                    numOfTasksHandeled++;
                    System.out.println(String.format("processing task #%d...", numOfTasksHandeled));

                    Review review = mapper.readValue(message.get(0).getBody(), Review.class);
                    Map<String, MessageAttributeValue> attributes = message.get(0).getMessageAttributes();

                    //ANALYSIS LOGIC
                    Integer reviewSentiment = findSentiment(review.getText());
                    String reviewEntities = printEntities(review.getText());
                    Integer rating = review.getRating() - 1;
                    String isSarcastic = Math.abs(rating - reviewSentiment) <= 2 ? "No" : "Yes";
                    String result = "Title: " + review.getTitle() + "\tReview: " + review.getText() +
                            "\tRating: " + review.getRating() + "\tAuthor: " + review.getAuthor() +
                            "\tLink: " + review.getLink() + "\tEntities: " + reviewEntities +
                            "\tSarcastic: " + isSarcastic + "\tSentiment: " + reviewSentiment;

                    //MESSAGE HANDLING
                    SQS.sendMessage(new SendMessageRequest()
                            .withQueueUrl(RESULTS_QUEUE_URL)
                            .withMessageBody(result)
                            .withMessageAttributes(attributes));

                    SQS.deleteMessage(TASKS_QUEUE_URL, message.get(0).getReceiptHandle());
                    System.out.println(String.format("successfully processed %d tasks", numOfTasksHandeled));
                }
            }catch (Exception e){
                numOfTasksHandeled--;
                System.out.println("failed to process task #" + numOfTasksHandeled);
                e.printStackTrace();
            }
        }
    }

    public static int findSentiment(String review) {
        Properties props = new Properties();
        props.put("annotators", "tokenize, ssplit, parse, sentiment");
        StanfordCoreNLP sentimentPipeline =  new StanfordCoreNLP(props);

        int mainSentiment = 0;
        if (review!= null && review.length() > 0) {
            int longest = 0;
            Annotation annotation = sentimentPipeline.process(review);
            for (CoreMap sentence : annotation
                    .get(CoreAnnotations.SentencesAnnotation.class)) {
                Tree tree = sentence
                        .get(SentimentCoreAnnotations.AnnotatedTree.class);
                int sentiment = RNNCoreAnnotations.getPredictedClass(tree);
                String partText = sentence.toString();
                if (partText.length() > longest) {
                    mainSentiment = sentiment;
                    longest = partText.length();
                }

            }
        }
        return mainSentiment;
    }

    public static String printEntities(String review){
        Properties props = new Properties();
        props.put("annotators", "tokenize , ssplit, pos, lemma, ner");
        StanfordCoreNLP NERPipeline =  new StanfordCoreNLP(props);
        StringBuilder entities = new StringBuilder();
        entities.append('[');
        // create an empty Annotation just with the given text
        Annotation document = new Annotation(review);

        // run all Annotators on this text
        NERPipeline.annotate(document);

        // these are all the sentences in this document
        // a CoreMap is essentially a Map that uses class objects as keys and has values with custom types
        List<CoreMap> sentences = document.get(SentencesAnnotation.class);

        for(CoreMap sentence: sentences) {
            // traversing the words in the current sentence
            // a CoreLabel is a CoreMap with additional token-specific methods
            for (CoreLabel token: sentence.get(TokensAnnotation.class)) {
                // this is the text of the token
                String word = token.get(TextAnnotation.class);
                // this is the NER label of the token
                String ne = token.get(NamedEntityTagAnnotation.class);

                if(ne.equals("PERSON") || ne.equals("LOCATION") || ne.equals("ORGANIZATION"))
                    entities.append(word).append(':').append(ne).append(',');
            }
        }
        if (entities.length() > 2)
            entities.deleteCharAt(entities.length()-1);
        entities.append(']');
        return entities.toString();
    }
}