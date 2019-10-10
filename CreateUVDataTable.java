package com.amazonaws.code;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;

public class CreateUVDataTable {

	static DynamoDB dynamoDB = new DynamoDB(new AmazonDynamoDBClient(new ProfileCredentialsProvider()));

	static SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
	
	static Date date = new Date();
	
//	static String TableName = "data_"+ date.toString().substring(25, 29)+"_"+date.toString().substring(8, 10)+"_"+date.toString().substring(4, 7) ;
	static String TableName = "data_UV_average";
	public static void main(String[] args) throws Exception {
		
		try {

			deleteTable(TableName);

			// Parameter1: table name // Parameter2: reads per second //
			// Parameter3: writes per second // Parameter4/5: partition key and data type
			// Parameter6/7: sort key and data type (if applicable)

			createTable(TableName, 10L, 5L, "city", "S");

		} catch (Exception e) {
			System.err.println("Program failed:");
			System.err.println(e.getMessage());
		}
		System.out.println("Success.");
	}

	private static void deleteTable(String tableName) {
		Table table = dynamoDB.getTable(tableName);
		try {
			System.out.println("Issuing DeleteTable request for " + tableName);
			table.delete();
			System.out.println("Waiting for " + tableName + " to be deleted...this may take a while...");
			table.waitForDelete();

		} catch (Exception e) {
			System.err.println("DeleteTable request failed for " + tableName);
			System.err.println(e.getMessage());
		}
	}

	private static void createTable(String tableName, long readCapacityUnits, long writeCapacityUnits,
			String partitionKeyName, String partitionKeyType) {

		createTable(tableName, readCapacityUnits, writeCapacityUnits, partitionKeyName, partitionKeyType, null, null);
	}

	private static void createTable(String tableName, long readCapacityUnits, long writeCapacityUnits,
			String partitionKeyName, String partitionKeyType, String sortKeyName1, String sortKeyType1) {

		try {

			ArrayList<KeySchemaElement> keySchema = new ArrayList<KeySchemaElement>();
			keySchema.add(new KeySchemaElement().withAttributeName(partitionKeyName).withKeyType(KeyType.HASH)); // Partition
																													// key

			ArrayList<AttributeDefinition> attributeDefinitions = new ArrayList<AttributeDefinition>();
			attributeDefinitions.add(
					new AttributeDefinition().withAttributeName(partitionKeyName).withAttributeType(partitionKeyType));

			if (sortKeyName1 != null) {
				keySchema.add(new KeySchemaElement().withAttributeName(sortKeyName1).withKeyType(KeyType.RANGE)); // Sort
																													// key
				attributeDefinitions
						.add(new AttributeDefinition().withAttributeName(sortKeyName1).withAttributeType(sortKeyType1));
			}

			CreateTableRequest request = new CreateTableRequest().withTableName(tableName).withKeySchema(keySchema)
					.withProvisionedThroughput(new ProvisionedThroughput().withReadCapacityUnits(readCapacityUnits)
							.withWriteCapacityUnits(writeCapacityUnits));

			request.setAttributeDefinitions(attributeDefinitions);

			System.out.println("Issuing CreateTable request for " + tableName);
			Table table = dynamoDB.createTable(request);
			System.out.println("Waiting for " + tableName + " to be created...this may take a while...");
			table.waitForActive();

		} catch (Exception e) {
			System.err.println("CreateTable request failed for " + tableName);
			System.err.println(e.getMessage());
		}
	}

}
