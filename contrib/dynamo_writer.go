package main

import (
	"fmt"
	"sync/atomic"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/segmentio/ksuid"
)

const SrcTableName = "ddb-sync-source"
const batchMax = 500

var recordCount int64

func main() {
	svc := dynamodb.New(session.New())
	for i := 0; i < batchMax; i++ {
		err := sendBatch(svc)
		if err != nil {
			fmt.Println("Err: ", err)
		}
	}
}

func sendBatch(svc *dynamodb.DynamoDB) error {
	input := &dynamodb.BatchWriteItemInput{
		RequestItems: buildRequestItems(),
	}

	atomic.AddInt64(&recordCount, 25)
	_, err := svc.BatchWriteItem(input)
	if err != nil {
		return err
	}
	fmt.Printf("Written %d records\n", atomic.LoadInt64(&recordCount))
	return nil
}

func buildRequestItems() map[string][]*dynamodb.WriteRequest {
	return map[string][]*dynamodb.WriteRequest{
		SrcTableName: getRequests(25),
	}
}

func getRequests(count int) []*dynamodb.WriteRequest {
	var requests []*dynamodb.WriteRequest
	for i := 0; i < count; i++ {
		request := dynamodb.WriteRequest{
			PutRequest: &dynamodb.PutRequest{
				Item: getItem(),
			},
		}

		requests = append(requests, &request)
	}
	return requests
}

func getItem() map[string]*dynamodb.AttributeValue {
	uid := ksuid.New().String()
	return map[string]*dynamodb.AttributeValue{
		"Artist": {
			S: aws.String(fmt.Sprintf("Artist like %s", uid)),
		},
		"SongTitle": {
			S: aws.String(fmt.Sprintf("SongTitle like %s", uid)),
		},
		"AlbumTitle": {
			S: aws.String(fmt.Sprintf("AlbumTitle like %s", uid)),
		},
		"DATA": {
			S: aws.String("Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum."),
		},
	}

}
