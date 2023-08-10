package kvslib

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type KVSClientAPI interface {
	GetItem(ctx context.Context, key int64) ([]byte, error)
	PutItem(ctx context.Context, key int64, value interface{}) error
}

type KVS struct {
	URL             string
	AccessKeyID     string
	SecretAccessKey string
	Region          string
}

type KVSClient struct {
	client    *dynamodb.Client
	tableName string
}

type kvsItem struct {
	Key   string `dynamodbav:"key"`
	Value []byte `dynamodbav:"value"`
}

func (kvs KVS) NewClient(name string) *KVSClient {
	if name == "" {
		return nil
	}

	sdkConfig, err := config.LoadDefaultConfig(context.TODO(),
		config.WithEndpointResolverWithOptions(aws.EndpointResolverWithOptionsFunc(
			func(service, region string, options ...interface{}) (aws.Endpoint, error) {
				return aws.Endpoint{URL: kvs.URL}, nil
			})),
		config.WithCredentialsProvider(credentials.StaticCredentialsProvider{
			Value: aws.Credentials{
				AccessKeyID: kvs.AccessKeyID, SecretAccessKey: kvs.SecretAccessKey,
			},
		}),
		config.WithDefaultRegion(kvs.Region),
	)
	if err != nil {
		log.Fatalf("unable to load SDK config, %v", err)
	}

	return &KVSClient{
		client:    dynamodb.NewFromConfig(sdkConfig),
		tableName: name,
	}
}

func (kvsClient KVSClient) BootStrap() error {
	exists, err := kvsClient.tableExists()
	if err != nil {
		return err
	}
	if !exists {
		log.Printf("creating table %s...\n", kvsClient.tableName)
		err = kvsClient.createTable()
		if err != nil {
			return err
		}

		log.Printf("created table %s.\n", kvsClient.tableName)
	} else {
		log.Printf("table %s already exists.\n", kvsClient.tableName)
	}

	return nil
}

func (kvsClient KVSClient) tableExists() (bool, error) {
	exists := true
	_, err := kvsClient.client.DescribeTable(
		context.TODO(), &dynamodb.DescribeTableInput{TableName: aws.String(kvsClient.tableName)},
	)
	if err != nil {
		var notFoundEx *types.ResourceNotFoundException
		if errors.As(err, &notFoundEx) {
			log.Printf("table %s does not exist.\n", kvsClient.tableName)
			err = nil
		} else {
			log.Printf("couldn't determine existence of table %s. Here's why: %s\n", kvsClient.tableName, err)
		}
		exists = false
	}
	return exists, err
}

func (kvsClient KVSClient) createTable() error {
	_, err := kvsClient.client.CreateTable(context.TODO(), &dynamodb.CreateTableInput{
		AttributeDefinitions: []types.AttributeDefinition{{
			AttributeName: aws.String("key"),
			AttributeType: types.ScalarAttributeTypeS,
		},
		},
		KeySchema: []types.KeySchemaElement{{
			AttributeName: aws.String("key"),
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
		Key:   strconv.FormatInt(key, 10),
		Value: bytes,
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
	item := kvsItem{Key: strconv.FormatInt(key, 10)}
	response, err := kvsClient.client.GetItem(context.TODO(), &dynamodb.GetItemInput{
		Key: item.getKey(), TableName: aws.String(kvsClient.tableName),
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

func (item kvsItem) getKey() map[string]types.AttributeValue {
	key, err := attributevalue.Marshal(item.Key)
	if err != nil {
		panic(err)
	}
	return map[string]types.AttributeValue{"key": key}
}
