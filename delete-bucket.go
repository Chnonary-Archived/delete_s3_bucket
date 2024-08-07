package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"os"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

var mu sync.Mutex

func main() {
	// Define command-line flags
	region := flag.String("region", "us-west-2", "AWS region")
	profile := flag.String("profile", "default", "AWS profile name")

	// Parse the command-line flags
	flag.Parse()

	// Load AWS configuration
	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion(*region), config.WithSharedConfigProfile(*profile))
	if err != nil {
		fmt.Printf("unable to load SDK config, %v", err)
		os.Exit(1)
	}

	// Create an S3 client
	s3Client := s3.NewFromConfig(cfg)

	// List all buckets
	buckets, err := listBuckets(context.TODO(), s3Client)
	if err != nil {
		fmt.Printf("failed to list buckets, %v", err)
		os.Exit(1)
	}

	for _, bucket := range buckets {
		bucketName := aws.ToString(bucket.Name)
		fmt.Printf("Found bucket: %s\n", bucketName)

		// Prompt user to confirm deletion
		var choice string
		fmt.Printf("Do you want to empty and delete this bucket? (y/n): ")
		fmt.Scanln(&choice)

		if choice == "y" || choice == "Y" {
			// Confirm deletion
			var confirm string
			fmt.Printf("Are you sure you want to delete bucket %s and all its contents? (y/n): ", bucketName)
			fmt.Scanln(&confirm)

			if confirm == "y" || confirm == "Y" {
				fmt.Printf("Deleting all objects from bucket %s...\n", bucketName)
				err = deleteAllObjects(context.TODO(), s3Client, bucketName)
				if err != nil {
					fmt.Printf("failed to delete all objects from bucket %s, %v", bucketName, err)
					continue
				}

				fmt.Println("Successfully deleted all objects from the bucket.")

				fmt.Printf("Deleting bucket %s...\n", bucketName)
				_, err = s3Client.DeleteBucket(context.TODO(), &s3.DeleteBucketInput{
					Bucket: aws.String(bucketName),
				})
				if err != nil {
					fmt.Printf("failed to delete bucket %s, %v", bucketName, err)
					continue
				}

				fmt.Println("Successfully deleted the bucket.")
			} else {
				fmt.Println("Bucket deletion canceled.")
			}
		} else {
			fmt.Println("Bucket deletion canceled.")
		}
	}
}

func listBuckets(ctx context.Context, s3Client *s3.Client) ([]types.Bucket, error) {
	resp, err := s3Client.ListBuckets(ctx, &s3.ListBucketsInput{})
	if err != nil {
		return nil, fmt.Errorf("unable to list buckets, %w", err)
	}
	return resp.Buckets, nil
}

func deleteAllObjects(ctx context.Context, s3Client *s3.Client, bucketName string) error {
	var wg sync.WaitGroup
	semaphore := make(chan struct{}, 10) // Limit concurrency to 10

	listObjectsInput := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
	}

	// Continuation token for paginated results
	var continuationToken *string

	for {
		if continuationToken != nil {
			listObjectsInput.ContinuationToken = continuationToken
		}

		// List objects
		listObjectsOutput, err := s3Client.ListObjectsV2(ctx, listObjectsInput)
		if err != nil {
			return fmt.Errorf("unable to list objects, %w", err)
		}

		if len(listObjectsOutput.Contents) == 0 {
			break // No more objects to process
		}

		for _, object := range listObjectsOutput.Contents {
			wg.Add(1)
			semaphore <- struct{}{} // Acquire semaphore

			go func(objectKey string) {
				defer wg.Done()
				defer func() { <-semaphore }() // Release semaphore

				_, err := s3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
					Bucket: aws.String(bucketName),
					Key:    aws.String(objectKey),
				})
				if err != nil {
					fmt.Printf("failed to delete object %s, %v\n", objectKey, err)
					return
				}
			}(aws.ToString(object.Key))
		}

		// Update continuation token
		continuationToken = listObjectsOutput.NextContinuationToken
	}

	// Wait for all deletions to complete
	wg.Wait()
	return nil
}
