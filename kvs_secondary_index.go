package kvslib

import (
	"context"
	"encoding/json"
	"log"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func (kvsClient KVSClient) createTableWithSecondaryIndex() error {
	_, err := kvsClient.client.CreateTable(context.TODO(), &dynamodb.CreateTableInput{
		AttributeDefinitions: []types.AttributeDefinition{
			{
				AttributeName: aws.String(primaryKey),
				AttributeType: types.ScalarAttributeTypeS,
			},
			{
				AttributeName: aws.String(secondaryKey),
				AttributeType: types.ScalarAttributeTypeS,
			},
		},
		KeySchema: []types.KeySchemaElement{
			{
				AttributeName: aws.String(primaryKey),
				KeyType:       types.KeyTypeHash,
			},
			{
				AttributeName: aws.String(secondaryKey),
				KeyType:       types.KeyTypeRange,
			},
		},
		GlobalSecondaryIndexes: []types.GlobalSecondaryIndex{{
			IndexName: aws.String(secondaryIndex),
			KeySchema: []types.KeySchemaElement{{
				AttributeName: aws.String(secondaryKey),
				KeyType:       types.KeyTypeHash,
			}},
			Projection: &types.Projection{
				ProjectionType: types.ProjectionTypeKeysOnly,
			},
			ProvisionedThroughput: &types.ProvisionedThroughput{
				ReadCapacityUnits:  aws.Int64(10),
				WriteCapacityUnits: aws.Int64(10),
			},
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

func (kvsClient KVSClient) PutItemWithSecondary(ctx context.Context, key int64, secondaryKey int64, value interface{}) error {
	bytes, err := json.Marshal(value)
	if err != nil {
		return err
	}

	secondaryKeyStr := strconv.FormatInt(secondaryKey, 10)
	item, err := attributevalue.MarshalMap(kvsItem{
		PrimaryKey:   strconv.FormatInt(key, 10),
		SecondaryKey: &secondaryKeyStr,
		Value:        bytes,
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

func (kvsClient KVSClient) GetItemsBySecondary(ctx context.Context, key int64) ([][]byte, error) {
	var err error
	var result [][]byte
	var response *dynamodb.QueryOutput
	keyEx := expression.Key(secondaryKey).Equal(expression.Value(key))
	expr, err := expression.NewBuilder().WithKeyCondition(keyEx).Build()
	if err != nil {
		log.Printf("Couldn't build expression for query. Here's why: %s\n", err)
	} else {
		response, err = kvsClient.client.Query(ctx, &dynamodb.QueryInput{
			TableName:                 aws.String(kvsClient.tableName),
			IndexName:                 aws.String(secondaryIndex),
			ExpressionAttributeNames:  expr.Names(),
			ExpressionAttributeValues: expr.Values(),
			KeyConditionExpression:    expr.KeyCondition(),
		})
		if err != nil {
			log.Printf("Couldn't query for items in %d. Here's why: %s\n", key, err)
		} else {
			result := make([][]byte, len(response.Items))
			for i, respItem := range response.Items {
				item := kvsItem{}
				err = attributevalue.UnmarshalMap(respItem, &item)
				result[i] = item.Value
			}
		}
	}
	return result, err
}
