// Copyright 2012-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// Licensed under the Apache License, Version 2.0.

package com.amazonaws.code;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.ScanOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.ScanSpec;
import com.amazonaws.services.dynamodbv2.document.utils.NameMap;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class uploadCitiesToS3 {

	// static DynamoDB dynamoDB = new DynamoDB(new AmazonDynamoDBClient(new
	// ProfileCredentialsProvider()));

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {
		AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().withRegion(Regions.US_EAST_1).build();

		DynamoDB dynamoDB = new DynamoDB(client);

		String TableName = "Allcities";
		String TableName2 = "50cities";

		Table table = dynamoDB.getTable(TableName);
		Table table2 = dynamoDB.getTable(TableName2);

		// read coordination from table 'cities' from DynamoDB
		ArrayList<String> content = new ArrayList<>();
		ArrayList<String> content2 = new ArrayList<>();
		HashMap<String, Coordination> coord = new HashMap<String, Coordination>();
		String arr[] = { "Sydney", "Melbourne", "Cairns", "Adelaide", "Brisbane", "Perth", "Canberra", "Darwin",
				"Hobart", "Gold Coast", "Alice Springs", "Newcastle", "Geelong", "Launceston", "Wollongong","Mackay",
				"Gladstone","Dampier", "Port Hedland", "Central Coast","Sunshine Coast","Townsville","Toowoomba",
				"Ballarat","Bendigo","Rockhampton","Bunbury","Taree","Bundaberg","Melton","Wagga Wagga",
				"Mildura","Port Macquarie","Tamworth","Orange","Busselton","Geraldton","Dubbo","Bathurst",
				"Warrnambool","Albany","Devonport","Mount Gambier","Lismore","Nelson Bay","Logan City","Rockingham",
				"Mandurah","Maitland"};
		ArrayList <String> ids=new ArrayList<String>();
		

		ScanSpec scanSpec = new ScanSpec().withProjectionExpression("id,city,country,latitude,longtitude");
		ScanSpec scanSpec2 = new ScanSpec().withProjectionExpression("id,city,latitude,longtitude");
		

		try {
			ItemCollection<ScanOutcome> items = table.scan(scanSpec);
			Iterator<Item> iterator = items.iterator();
			System.out.println("Read data from " + TableName + "");
			while (iterator.hasNext()) {
				Item item = iterator.next();
				content.add(item.toJSON());
			}
			for (int i = 0; i < content.size(); i++) {
				ObjectMapper mapper = new ObjectMapper();
				JsonNode actualObj = mapper.readTree(content.get(i));
				String city = actualObj.path("city").asText();
				for(int j=0;j<arr.length;j++) {
					if(city.equals(arr[j])) {
						double lon = actualObj.path("longtitude").asDouble();
						double lat = actualObj.path("latitude").asDouble();
						String id = actualObj.path("id").asText();
						ids.add(id);
						System.out.println(city);
						System.out.println(lat);
						System.out.println(lon);
						Coordination co = new Coordination(lat, lon);
						coord.put(city, co);
					}
				}
			}
		} catch (Exception e) {
			System.err.println("Unable to read...");
			System.err.println(e.getMessage());
		}
		
		for (int a = 0; a < coord.size(); a++) {// each city
			double lat = coord.get(arr[a]).getLat();
			double lon = coord.get(arr[a]).getLon();
			String city = arr[a];
			String id = ids.get(a);
			try {
				table2.putItem(new Item().withPrimaryKey("city", city).withString("id", id).withNumber("latitude", lat ).withNumber("longtitude", lon));
				System.out.println("PutItem succeeded: " + city);

			} catch (Exception e) {
				System.err.println("Unable to add site: " + city);
				System.err.println(e.getMessage());
				break;
			}


		}
	
		
		try {
			ItemCollection<ScanOutcome> items2 = table2.scan(scanSpec);
			Iterator<Item> iterator2 = items2.iterator();
			System.out.println("Read data from " + TableName2 + "");
			while (iterator2.hasNext()) {
				Item item2 = iterator2.next();
				content2.add(item2.toJSON());
			}
		} catch (Exception e) {
			System.err.println("Unable to read...");
			System.err.println(e.getMessage());
		}
		

		String fileName = "50cities.json";
		try {
			FileOutputStream fo = new FileOutputStream(fileName);
			OutputStreamWriter os = new OutputStreamWriter(fo, "UTF-8");
			os.write(content2.toString());
			os.flush();
			os.close();
			System.out.println("File created successfully!");
		} catch (IOException ex) {
			ex.printStackTrace();
		}
		// String clientRegion = "us-east-1";
		String bucketName = "thistest111";
		String stringObjKeyName = "cc2/" + fileName;
		String fileObjKeyName = "cc2/" + fileName;

		try {
			AmazonS3 s3Client = new AmazonS3Client().withRegion(Regions.US_EAST_1);

			// Upload a text string as a new object.
			s3Client.putObject(bucketName, stringObjKeyName, "Uploaded String Object");

			// Upload a file as a new object with ContentType and title specified.
			PutObjectRequest request = new PutObjectRequest(bucketName, fileObjKeyName, new File(fileName));
			ObjectMetadata metadata = new ObjectMetadata();
			metadata.setContentType("plain/text");
			metadata.addUserMetadata("x-amz-meta-title", "someTitle");
			request.setMetadata(metadata);
			s3Client.putObject(request);
		} catch (AmazonServiceException e) {
			System.out.println(e.getMessage());
			// The call was transmitted successfully, but Amazon S3 couldn't process
			// it, so it returned an error response.
			e.printStackTrace();
		} catch (SdkClientException e) {
			System.out.println(e.getMessage());
			// Amazon S3 couldn't be contacted for a response, or the client
			// couldn't parse the response from Amazon S3.
			e.printStackTrace();
		}
	}

}