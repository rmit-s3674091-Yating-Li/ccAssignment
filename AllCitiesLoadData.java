package com.amazonaws.code;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class AllCitiesLoadData {

	static DynamoDB dynamoDB = new DynamoDB(new AmazonDynamoDBClient(new ProfileCredentialsProvider()));
	

	public static void main(String[] args) throws Exception {

		Table table = dynamoDB.getTable("Allcities");
		File file = new File("/Users/haruka/Desktop/ccA2/city.list.json");
		FileReader fileReader = new FileReader(file);
		Reader reader = new InputStreamReader(new FileInputStream(file),"utf-8");
	
		JsonParser parser = new JsonFactory().createParser(reader);
		ObjectMapper mapper = new ObjectMapper();
		mapper.setSerializationInclusion(Include.NON_NULL); 
		JsonNode rootNode = mapper.readTree(parser);
		Iterator<JsonNode> iter = rootNode.iterator();
		ArrayList <String> cityNames = new ArrayList<String>();
		int index=0;

		while (iter.hasNext()) {
			JsonNode results = (JsonNode) iter.next();
			String id = results.path("id").asText();
			String name = results.path("name").asText();
			String country = results.path("country").asText();
			double latitude = results.path("coord").path("lat").asDouble();
			double longitude = results.path("coord").path("lon").asDouble();
			if(!checkDuplicate(cityNames,name)&&country.equals("AU")) {
				
//				System.out.println(id+";"+name+";"+country+";"+latitude+";"+longitude);
				
				cityNames.add(name);
				index++;

				try {
					table.putItem(new Item().withPrimaryKey("id", id).withString("name", name).withString("country",country)
							.withNumber("latitude", latitude).withNumber("longtitude", longitude));
					System.out.println("PutItem succeeded: " + name + " " + country + " " + longitude + " " + latitude);

				} catch (Exception e) {
					System.err.println("Unable to add site: " + name + " " + country + " " + longitude + " " + latitude);
					System.err.println(e.getMessage());
					break;
				}
						
			}
			
		}
		reader.close();
		parser.close();
	}
	
	public static boolean checkDuplicate(ArrayList<String> cityNames, String name) {
		boolean duplicate=false;
		if(cityNames.size()!=0) {
			for(int i=0;i<cityNames.size();i++) {
				if(cityNames.get(i).equals(name)) {
					duplicate=true;
				}
			}
		}
		
		
		return duplicate;
		
	}
	
	
	
}
