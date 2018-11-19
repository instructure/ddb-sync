/*
 * ddb-sync
 * Copyright (C) 2018 Instructure Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package main

import (
	"fmt"
	"os"
	"sync/atomic"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/segmentio/ksuid"
)

var recordCount int64

const (
	defaultTableName = "ddb-sync-source"
	batchMax         = 500
)

func main() {

	destTableName, err := parseArgs()
	if err != nil {
		fmt.Printf("[ERROR] %v\n", err)
		os.Exit(3)
	}

	svc := dynamodb.New(session.New())
	for i := 0; i < batchMax; i++ {
		err := sendBatch(svc, destTableName)
		if err != nil {
			fmt.Println("Err: ", err)
		}
	}
}

func parseArgs() (string, error) {
	if len(os.Args) > 2 {
		return "", fmt.Errorf("Wrong number of arguments")
	}

	if len(os.Args) > 1 {
		return os.Args[1], nil
	}

	return defaultTableName, nil
}

func sendBatch(svc *dynamodb.DynamoDB, destTableName string) error {
	input := &dynamodb.BatchWriteItemInput{
		RequestItems: buildRequestItems(destTableName),
	}

	atomic.AddInt64(&recordCount, 25)
	_, err := svc.BatchWriteItem(input)
	if err != nil {
		return err
	}
	fmt.Printf("Written %d records\n", atomic.LoadInt64(&recordCount))
	return nil
}

func buildRequestItems(destTableName string) map[string][]*dynamodb.WriteRequest {
	return map[string][]*dynamodb.WriteRequest{
		destTableName: getRequests(25),
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
