//package bgu.dsp.sarcasm.worker.test;
//
//import bgu.dsp.sarcasm.common.AWSClients;
//import bgu.dsp.sarcasm.common.Review;
//import bgu.dsp.sarcasm.worker.Worker;
//import com.amazonaws.services.sqs.AmazonSQS;
//import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
//import com.amazonaws.services.sqs.model.MessageAttributeValue;
//import com.amazonaws.services.sqs.model.SendMessageRequest;
//import org.junit.Test;
//
//import java.util.HashMap;
//import java.util.Map;
//
//public class WorkerTest {
//
//    @Test
//    public void testWorker(){
//        AmazonSQS sqs = AmazonSQSClientBuilder.defaultClient();
//        String tasksQueueUrl = sqs.getQueueUrl("tasksQueue").getQueueUrl();
//
//        Map<String, MessageAttributeValue> taskAttributes = new HashMap<>();
//        taskAttributes.put("filename" , new MessageAttributeValue().withDataType("String").withStringValue("Test1"));
//
//        sqs.sendMessage(new SendMessageRequest()
//            .withQueueUrl(tasksQueueUrl)
//            .withMessageAttributes(taskAttributes)
//            .withMessageBody("{\"id\":\"R1IKZK5S0DCKZ0\",\"link\":\"https://www.amazon.com/gp/customer-reviews/R1IKZK5S0DCKZ0/ref=cm_cr_arp_d_rvw_ttl?ie=UTF8&ASIN=0689835604\",\"title\":\"Super cute!\",\"text\":\"My daughter loves lifting the flaps herself! She's almost a year. Great colorful pictures. I'm never disappointed with Karen Katz!\",\"rating\":5,\"author\":\"Jacobandem\",\"date\":\"2016-06-11T21:00:00.000Z\"}"));
//
//        try {
//            Worker.main(null);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }
//}
