package com.lahoti.kafkastream.candidate;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;


public class StreamCandidateMessages {
	
	private static Logger logger = LoggerFactory.getLogger(StreamCandidateMessages.class.getName());
	
	public static void main(String args[]){
	//create properties
	Properties properties  = new Properties();
	properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
	properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG,"demo-kakfa-streams");
	properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,Serdes.StringSerde.class.getName());
	properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,Serdes.StringSerde.class.getName());
	
	//create topology
	StreamsBuilder streamsBuilder = new StreamsBuilder();
	//Input Topic
	String topicToRead = "candidate-update";
	String topicToWrite = "transaction-log";
	KStream<String, String> inputTopic = streamsBuilder.stream(topicToRead);
	//Print the messages to the console
	inputTopic.foreach((key, value) -> logger.info(key + " => " + value));
	//Filter the message
	KStream<String, String> transformStream = inputTopic.mapValues(value -> transformJsonMessage(value));
	
	transformStream.to(topicToWrite);
	
	//build the topology
	KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), properties);
	
	//start the stream application
	kafkaStreams.start();
	}
	
	private static String transformJsonMessage(String value){
		String transformMessage = "";	
		JsonObject jsonObject = new Gson().fromJson(value, JsonObject.class);
		
		JsonObject newValue = new JsonObject();
		newValue.addProperty("recordId", jsonObject.get("recordId").toString()); 
		newValue.addProperty("event", jsonObject.get("event").toString());
		newValue.addProperty("status", "SUCCESS");
		
		Gson gson = new Gson();
		transformMessage = gson.toJson(newValue);
	
		return transformMessage;
	}

}
