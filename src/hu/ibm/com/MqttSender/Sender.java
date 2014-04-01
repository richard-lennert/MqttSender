package hu.ibm.com.MqttSender;

import com.ibm.micro.client.mqttv3.MqttClient;
import com.ibm.micro.client.mqttv3.MqttDeliveryToken;
import com.ibm.micro.client.mqttv3.MqttException;
import com.ibm.micro.client.mqttv3.MqttMessage;
import com.ibm.micro.client.mqttv3.MqttTopic;

public class Sender {
	// Because MQTT protocol requires a client id, we define one
	private static final String CLIENT_ID = "Java-MQTT-Sender";
	
	// This is the endpoint provided by MessageSight, we are using the default demo endpoint at this time (DemoHub > DemoMqttEndpoint)
	private static final String ENDPOINT = "tcp://10.106.12.12:1883";
	
	// This is the topic name where the code will put the messages. It assumes a Pub-Sub messaging topology
	private static final String TOPIC [] = {"java0", "java1", "java2"};
	
	// Number of the messages going to be produced
	private static final int messageCount = 100000;

	public static void main(String[] args) {
		
		// Define an MQTT client
		MqttClient client = null;
		
		try {
			// Construct the client
			client = new MqttClient(ENDPOINT, CLIENT_ID);
			
			// Subscribe to the defined topic
			MqttTopic topic;
			
			// Define the message object
			MqttMessage msg = null;

			// Connecting the client to the endpoint
			client.connect();

			// Sending the required number of messages
			for (int i = 0; i < messageCount; i++) {
				
				topic = client.getTopic(TOPIC[i%3]);
				
				System.out.println("Sending message #" + i);
                      // Construct the message with the message content
		                msg = new MqttMessage(("HelloWorklight!").getBytes());
				
				// Setting QoS level (can be 0, 1 and 2, deatils in the product documentation) 
				msg.setQos(0);

				// Sending the message to the topic
				MqttDeliveryToken token = topic.publish(msg);

				// We are waiting the acknowledge maximum 10 seconds
				token.waitForCompletion(10 * 1000); // milliseconds

				System.out.println("Message sent");
				System.out.println();
			}
		} catch (MqttException e) {
			e.printStackTrace();
		} finally {
			// When all the messages sent, disconnecting
			try {
				// Disconnect
				client.disconnect();
			} catch (MqttException e) {
				e.printStackTrace();
			}
		}
	}

}