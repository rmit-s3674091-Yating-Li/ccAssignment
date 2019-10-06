// Copyright 2012-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// Licensed under the Apache License, Version 2.0.

package com.amazonaws.code;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
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

public class upload15CitiesToS3 {

	// static DynamoDB dynamoDB = new DynamoDB(new AmazonDynamoDBClient(new
	// ProfileCredentialsProvider()));

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {
		AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().withRegion(Regions.US_EAST_1).build();

		DynamoDB dynamoDB = new DynamoDB(client);

		
		String TableName = "cities";

		Table table = dynamoDB.getTable(TableName);

		ArrayList<String> content = new ArrayList<>();

		ScanSpec scanSpec = new ScanSpec().withProjectionExpression("id,city,country,latitude,longtitude");

		try {
			ItemCollection<ScanOutcome> items = table.scan(scanSpec);
			Iterator<Item> iterator = items.iterator();
			System.out.println("Read cities from " + TableName + "");
			while (iterator.hasNext()) {
				 Item item = iterator.next();
				 content.add(item.toJSON());
	            
			}
			 System.out.println(content.toString());

		} catch (Exception e) {
			System.err.println("Unable to read...");
			System.err.println(e.getMessage());
		}

		String fileName = "15cities.json";
		try {
			FileOutputStream fo = new FileOutputStream(fileName);
			OutputStreamWriter os = new OutputStreamWriter(fo, "UTF-8");
			os.write(content.toString());
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