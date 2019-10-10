package com.amazonaws.code;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URL;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.ScanOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.ScanSpec;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;

public class Analyse {

	static Date date = new Date();

//	static String TableName = "data_"+ date.toString().substring(25, 29)+"_"+date.toString().substring(8, 10)+"_"+date.toString().substring(4, 7) ;
	static String TableName = "data_average";
	static DynamoDB dynamoDB = new DynamoDB(new AmazonDynamoDBClient(new ProfileCredentialsProvider()));

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {
		
//		AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().withRegion(Regions.US_EAST_1).build();

//		DynamoDB dynamoDB = new DynamoDB(client);

		Table table = dynamoDB.getTable(TableName);

		// read data from api
		// array stored with 15 cities names.
		String arr[] = { "Sydney", "Melbourne", "Cairns", "Adelaide", "Brisbane", "Perth", "Canberra", "Darwin",
				"Hobart", "Gold Coast", "Alice Springs", "Newcastle", "Geelong", "Launceston", "Wollongong" };
		String key = "c31c30bf92f140deeb023ccb57878bec";
		for (int i = 0; i < arr.length; i++) {// each city
			//read weather api
			URL url1 = new URL("https://api.openweathermap.org/data/2.5/forecast?q=" + arr[i] + "&appid=" + key);
			InputStreamReader reader = new InputStreamReader(url1.openStream(), "UTF-8");
			JsonParser parser = new JsonFactory().createParser(reader);

			JsonNode rootNode = new ObjectMapper().readTree(parser);
			Iterator<JsonNode> iter = rootNode.iterator();

//			JsonNode results = (JsonNode) iter.next();
			String city = rootNode.get("city").at("/name").asText();
			System.out.println(city);
			JsonNode currentNode = rootNode.get("list");
			ArrayNode arrayNode = (ArrayNode) currentNode;
			Iterator<JsonNode> node = arrayNode.elements();

			ArrayList<Double> currTemp = new ArrayList<Double>();
			ArrayList<String> icons = new ArrayList<String>();
			while (node.hasNext()) {
				JsonNode test = node.next();
				double temp = test.path("main").at("/temp").asDouble();
				currTemp.add(temp);
				String iconNum = test.path("weather").at("/0").at("/id").asText();
				icons.add(iconNum);

			}

			double sumMaxTemp = 0;
			double sumMinTemp = 0;
			double sumRain = 0;
			System.out.println("City " + arr[i]);
			for (int a = 0; a < 5; a++) {
				double maxTemp = -1000;
				double minTemp = 1000;
				int rain = 0;	
				for (int b = 0; b < 8; b++) {
					if (currTemp.get(b + a * 8) > maxTemp) {
						maxTemp = currTemp.get(b + a * 8);
					} else if (currTemp.get(b + a * 8) < minTemp) {
						minTemp = currTemp.get(b + a * 8);
					}
					
					if (icons.get(b + a * 8).startsWith("5")) {
						rain++;
					}
				}
				sumMaxTemp += maxTemp;
				sumMinTemp += minTemp;
				sumRain += rain/8;
				
//				System.out.println(minTemp);
//				System.out.println("Rain possibility: " + (rain / 8) * 100 + "%");

			}
			double aveMaxTemp = Math.floor((sumMaxTemp/5)-273.15);
			double aveMinTemp = Math.floor((sumMinTemp/5)-273.15);
			double aveRainP = (sumRain/5)*100;
			
			System.out.println("Average maxTemp: " + Math.floor((sumMaxTemp/5)-273.15));
			System.out.println("Average minTemp: " + Math.floor((sumMinTemp/5)-273.15));
			System.out.println("Average rain possibility: " + (sumRain/5)*100+"%");
			
			try {
				table.putItem(new Item().withPrimaryKey("city", city).withNumber("aveMaxTemp", aveMaxTemp)
						.withNumber("aveMinTemp", aveMinTemp).withNumber("rainPossibility", aveRainP));
				System.out.println("PutItem succeeded: " + city);

			} catch (Exception e) {
				System.err.println("Unable to add site: " + city);
				System.err.println(e.getMessage());
				break;
			}

			reader.close();
			parser.close();

		}

		// write Dynamo data to S3
		ArrayList<String> content = new ArrayList<>();

		ScanSpec scanSpec = new ScanSpec().withProjectionExpression("city,aveMaxTemp,aveMinTemp,rainPossibility");

		try {
			ItemCollection<ScanOutcome> items = table.scan(scanSpec);
			Iterator<Item> iterator = items.iterator();
			System.out.println("Read data from " + TableName + "");
			while (iterator.hasNext()) {
				Item item = iterator.next();
				content.add(item.toJSON());

			}
			System.out.println(content.toString());

		} catch (Exception e) {
			System.err.println("Unable to read...");
			System.err.println(e.getMessage());
		}

		String fileName = "data_weather.json";
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