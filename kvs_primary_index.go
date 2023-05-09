package kvslib

import (
	"context"
	"encoding/json"
	"log"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type kvsItem struct {
	PrimaryKey   string  `dynamodbav:"primary_key"`
	SecondaryKey *string `dynamodbav:"secodanry_key"`
	Value        []byte  `dynamodbav:"value"`
}

func (kvsClient KVSClient) createTable() error {
	_, err := kvsClient.client.CreateTable(context.TODO(), &dynamodb.CreateTableInput{
		AttributeDefinitions: []types.AttributeDefinition{{
			AttributeName: aws.String(primaryKey),
			AttributeType: types.ScalarAttributeTypeS,
		},
		},
		KeySchema: []types.KeySchemaElement{{
			AttributeName: aws.String(primaryKey),
			KeyType:       types.KeyTypeHash,
		}},
		TableName: aws.String(kvsClient.tableName),
		ProvisionedThroughput: &types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(10),
			WriteCapacityUnits: aws.Int64(10),
		},
	})
	if err != nil {
		log.Printf("couldn't create table %s. Here's why: %s\n", kvsClient.tableName, err)
	} else {
		waiter := dynamodb.NewTableExistsWaiter(kvsClient.client)
		err = waiter.Wait(context.TODO(), &dynamodb.DescribeTableInput{
			TableName: aws.String(kvsClient.tableName)}, 5*time.Minute)
		if err != nil {
			log.Printf("wait for table exists failed. Here's why: %s\n", err)
		}
	}
	return err
}

func (kvsClient KVSClient) PutItem(ctx context.Context, key int64, value interface{}) error {
	bytes, err := json.Marshal(value)
	if err != nil {
		return err
	}

	item, err := attributevalue.MarshalMap(kvsItem{
		PrimaryKey: strconv.FormatInt(key, 10),
		Value:      bytes,
	})
	if err != nil {
		return err
	}
	_, err = kvsClient.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(kvsClient.tableName),
		Item:      item,
	})
	if err != nil {
		log.Printf("Couldn't add item to table. Here's why: %v\n", err)
	}
	return err
}

func (kvsClient KVSClient) GetItem(ctx context.Context, key int64) ([]byte, error) {
	item := kvsItem{PrimaryKey: strconv.FormatInt(key, 10)}
	response, err := kvsClient.client.GetItem(context.TODO(), &dynamodb.GetItemInput{
		Key: item.getPrimaryKey(), TableName: aws.String(kvsClient.tableName),
	})
	if err != nil {
		log.Printf("couldn't get info about %d. Here's why: %s\n", key, err)
	} else {
		err = attributevalue.UnmarshalMap(response.Item, &item)
		if err != nil {
			log.Printf("couldn't unmarshal response. Here's why: %v\n", err)
		}
	}
	return item.Value, err
}
