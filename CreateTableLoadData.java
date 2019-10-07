package com.amazonaws.codesamples;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.TimeZone;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.LocalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.Projection;
import com.amazonaws.services.dynamodbv2.model.ProjectionType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;

public class CreateTablesLoadData {

    static DynamoDB dynamoDB = new DynamoDB(new AmazonDynamoDBClient(
            new ProfileCredentialsProvider()));

    static SimpleDateFormat dateFormatter = new SimpleDateFormat(
            "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

    static String citiesTableName = "Cities";
   

    public static void main(String[] args) throws Exception {

        try {

            deleteTable(citiesTableName);
           

            // Parameter1: table name // Parameter2: reads per second //
            // Parameter3: writes per second // Parameter4/5: partition key and data type
            // Parameter6/7: sort key and data type (if applicable)

            createTable(citiesTableName, 10L, 5L, "Id", "N");
            

            loadSampleProducts(productCatalogTableName);
           

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
            System.out.println("Waiting for " + tableName
                + " to be deleted...this may take a while...");
            table.waitForDelete();

        } catch (Exception e) {
            System.err.println("DeleteTable request failed for " + tableName);
            System.err.println(e.getMessage());
        }
    }

    private static void createTable(
        String tableName, long readCapacityUnits, long writeCapacityUnits, 
        String partitionKeyName, String partitionKeyType) {

        createTable(tableName, readCapacityUnits, writeCapacityUnits,
            partitionKeyName, partitionKeyType, null, null);
    }

    private static void createTable(
        String tableName, long readCapacityUnits, long writeCapacityUnits, 
        String partitionKeyName, String partitionKeyType, 
        String sortKeyName, String sortKeyType) {

        try {

            ArrayList<KeySchemaElement> keySchema = new ArrayList<KeySchemaElement>();
            keySchema.add(new KeySchemaElement()
                .withAttributeName(partitionKeyName)
                .withKeyType(KeyType.HASH)); //Partition key
            
            ArrayList<AttributeDefinition> attributeDefinitions = new ArrayList<AttributeDefinition>();
            attributeDefinitions.add(new AttributeDefinition()
                .withAttributeName(partitionKeyName)
                .withAttributeType(partitionKeyType));

            if (sortKeyName != null) {
                keySchema.add(new KeySchemaElement()
                    .withAttributeName(sortKeyName)
                    .withKeyType(KeyType.RANGE)); //Sort key
                attributeDefinitions.add(new AttributeDefinition()
                    .withAttributeName(sortKeyName)
                    .withAttributeType(sortKeyType));
            }

            CreateTableRequest request = new CreateTableRequest()
                    .withTableName(tableName)
                    .withKeySchema(keySchema)
                    .withProvisionedThroughput( new ProvisionedThroughput()
                        .withReadCapacityUnits(readCapacityUnits)
                        .withWriteCapacityUnits(writeCapacityUnits));

            // If this is the Reply table, define a local secondary index
            if (replyTableName.equals(tableName)) {
                
                attributeDefinitions.add(new AttributeDefinition()
                    .withAttributeName("PostedBy")
                    .withAttributeType("S"));

                ArrayList<LocalSecondaryIndex> localSecondaryIndexes = new ArrayList<LocalSecondaryIndex>();
                localSecondaryIndexes.add(new LocalSecondaryIndex()
                    .withIndexName("PostedBy-Index")
                    .withKeySchema(
                        new KeySchemaElement().withAttributeName(partitionKeyName).withKeyType(KeyType.HASH),  //Partition key
                        new KeySchemaElement() .withAttributeName("PostedBy") .withKeyType(KeyType.RANGE))  //Sort key
                    .withProjection(new Projection() .withProjectionType(ProjectionType.KEYS_ONLY)));

                request.setLocalSecondaryIndexes(localSecondaryIndexes);
            }

            request.setAttributeDefinitions(attributeDefinitions);

            System.out.println("Issuing CreateTable request for " + tableName);
            Table table = dynamoDB.createTable(request);
            System.out.println("Waiting for " + tableName
                + " to be created...this may take a while...");
            table.waitForActive();

        } catch (Exception e) {
            System.err.println("CreateTable request failed for " + tableName);
            System.err.println(e.getMessage());
        }
    }

    
}