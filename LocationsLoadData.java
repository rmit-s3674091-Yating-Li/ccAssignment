//package com.amazonaws.code;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.util.Iterator;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class LocationsLoadData {

//	static DynamoDB dynamoDB = new DynamoDB(new AmazonDynamoDBClient(new ProfileCredentialsProvider()));
	// static AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
	// .withEndpointConfiguration(new
	// AwsClientBuilder.EndpointConfiguration("http://localhost:8000", "us-west2"))
	// .build();
	//
	// static DynamoDB dynamoDB = new DynamoDB(client);

	public static void main(String[] args) throws Exception {

//		Table table = dynamoDB.getTable("Cities");
		String path = LocationsLoadData.class.getClassLoader().getResource("city.list.json").getPath();
		FileReader fileReader = new FileReader("path");
		Reader reader = new InputStreamReader(new FilewInputStream(file),"utf-8");
	
		JsonParser parser = new JsonFactory().createParser(reader);

		JsonNode rootNode = new ObjectMapper().readTree(parser);
		Iterator<JsonNode> iter = rootNode.iterator();

		JsonNode currentNode;

		Iterator<JsonNode> iter2 = currentNode.iterator();

		while (iter2.hasNext()) {
			JsonNode results = (JsonNode) iter2.next();
			String id = results.path("id").asText();
			String name = results.path("name").asText();
			String country = results.path("country").asText();
			double latitude = results.path("coordinates").at("/lat").asDouble();
			double longitude = results.path("coordinates").at("/lon").asDouble();
			System.out.println(name+";"+country+";"+latitude+";"+longitude+";");

//			try {
//				table.putItem(new Item().withPrimaryKey("id", id).withString("name", name).withString("country",country)
//						.withNumber("latitude", latitude).withNumber("longtitude", longitude));
//				System.out.println("PutItem succeeded: " + name + " " + longitude + " " + latitude);
//
//			} catch (Exception e) {
//				System.err.println("Unable to add site: " + location + " " + longitude + " " + latitude);
//				System.err.println(e.getMessage());
//				break;
//			}
		}
		reader.close();
		parser.close();
	}
	
	
	
}