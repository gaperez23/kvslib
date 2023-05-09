package kvslib

import (
	"context"
	"errors"
	"log"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

const (
	primaryKey   = "primary_key"
	secondaryKey = "secondary_key"
)

type KVS struct {
	URL             string
	AccessKeyID     string
	SecretAccessKey string
	Region          string
}

type KVSClient struct {
	client         *dynamodb.Client
	tableName      string
	secondaryIndex bool
}

func (kvs KVS) NewClient(name string) *KVSClient {
	return kvs.newClient(name, false)
}

func (kvs KVS) NewClientWithSecondaryIndex(name string) *KVSClient {
	return kvs.newClient(name, true)
}

func (kvs KVS) newClient(name string, secondaryIndex bool) *KVSClient {
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
		client:         dynamodb.NewFromConfig(sdkConfig),
		tableName:      name,
		secondaryIndex: secondaryIndex,
	}
}

func (kvsClient KVSClient) BootStrap() error {
	exists, err := kvsClient.tableExists()
	if err != nil {
		return err
	}
	if !exists {
		log.Printf("creating table %s...\n", kvsClient.tableName)

		if kvsClient.secondaryIndex {
			err = kvsClient.createTableWithSecondaryIndex()
		} else {
			err = kvsClient.createTable()
		}
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

func (item kvsItem) getPrimaryKey() map[string]types.AttributeValue {
	key, err := attributevalue.Marshal(item.PrimaryKey)
	if err != nil {
		panic(err)
	}
	return map[string]types.AttributeValue{primaryKey: key}
}

// func (item kvsItem) getSecondaryKey() map[string]types.AttributeValue {
// 	key, err := attributevalue.Marshal(*item.SecondaryKey)
// 	if err != nil {
// 		panic(err)
// 	}
// 	return map[string]types.AttributeValue{secondaryKey: key}
// }
