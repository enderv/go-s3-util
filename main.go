package main

import (
	"flag"
	"fmt"
	"log"
	"os/user"
	"path/filepath"
	"time"

	"github.com/alyu/configparser"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

func main() {
	sourceProfile := flag.String("p", "default", "Profile to use")
	sourceBucket := flag.String("s", "source", "Source Bucket")
	destinationBucket := flag.String("d", "destination", "Destination Bucket")
	newPrefix := flag.String("n", "", "new prefix")
	olderThan := flag.Int("o", 30, "older than")
	skipProfile := flag.Bool("k", false, "Skip profile check and just use default for use when no cred file and default will work")
	credFile := flag.String("c", filepath.Join(getCredentialPath(), ".aws", "credentials"), "Full path to credentials file")
	flag.Parse()
	if *sourceBucket == "source" {
		fmt.Println("You must specify a source bucket")
		return
	}
	if *destinationBucket == "destination" {
		fmt.Println("You must specify a destination bucket")
		return
	}
	if *newPrefix == "prefix" {
		fmt.Println("No prefix specified")
	}
	expiresAt := time.Now().AddDate(0, 0, 0-*olderThan)
	var sess *session.Session
	if *skipProfile {
		//Use Default Credentials
		sess = session.Must(session.NewSession())
	} else {
		//Get Specified Credentials
		exists, err := checkProfileExists(credFile, sourceProfile)
		if err != nil || !exists {
			fmt.Println(err.Error())
			return
		}
		sess = CreateSession(sourceProfile)
	}
	fmt.Println("Checking for objects older than " + expiresAt.Format(time.RFC3339))
	keysToMove, err := listObjects(sess, sourceBucket, &expiresAt)

	if err != nil {
		fmt.Println(err.Error())
		return
	}

	movedKeys := copyObjects(sess, sourceBucket, destinationBucket, newPrefix, keysToMove)
	deletedKeys := deleteOldObjects(sess, sourceBucket, movedKeys)
	for _, element := range deletedKeys {
		fmt.Println("Deleted: " + *element)
	}
}

// CreateSession Creates AWS Session with specified profile
func CreateSession(profileName *string) *session.Session {
	profileNameValue := *profileName
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		Profile: profileNameValue,
		Config:  aws.Config{Region: aws.String("us-east-1")},
	}))
	return sess
}

// getCredentialPath returns the users home directory path as a string
func getCredentialPath() string {
	usr, err := user.Current()
	if err != nil {
		log.Fatal(err)
	}
	return usr.HomeDir
}

// checkProfileExists takes path to the credentials file and profile name to search for
// Returns bool and any errors
func checkProfileExists(credFile *string, profileName *string) (bool, error) {
	config, err := configparser.Read(*credFile)
	if err != nil {
		fmt.Println("Could not find credentials file")
		fmt.Println(err.Error())
		return false, err
	}
	section, err := config.Section(*profileName)
	if err != nil {
		fmt.Println("Could not find profile in credentials file")
		return false, nil
	}
	if !section.Exists("aws_access_key_id") {
		fmt.Println("Could not find access key in profile")
		return false, nil
	}

	return true, nil
}

func listObjects(sess *session.Session, bucket *string, expirationTime *time.Time) ([]*string, error) {
	svc := s3.New(sess)
	helper := int64(100)
	input := &s3.ListObjectsV2Input{
		Bucket:  bucket,
		MaxKeys: &helper,
	}
	var keys []*string
	err := svc.ListObjectsV2Pages(input, func(page *s3.ListObjectsV2Output, lastPage bool) bool {
		for _, element := range page.Contents {
			if element.LastModified.Before(*expirationTime) {
				keys = append(keys, element.Key)
			}
		}
		return lastPage
	})
	return keys, err
}

func copyObjects(sess *session.Session, sourceBucket *string, destinationBucket *string, prefix *string, keys []*string) []*string {
	svc := s3.New(sess)
	var successfulCopies []*string
	for _, element := range keys {
		input := &s3.CopyObjectInput{
			Bucket:     aws.String(*destinationBucket),
			CopySource: aws.String("/" + *sourceBucket + "/" + *element),
			Key:        aws.String(*prefix + *element),
		}
		_, err := svc.CopyObject(input)
		if err != nil {
			if aerr, ok := err.(awserr.Error); ok {
				switch aerr.Code() {
				case s3.ErrCodeObjectNotInActiveTierError:
					fmt.Println(s3.ErrCodeObjectNotInActiveTierError, aerr.Error())
				default:
					fmt.Println(aerr.Error())
				}
			} else {
				// Print the error, cast err to awserr.Error to get the Code and
				// Message from an error.
				fmt.Println(err.Error())
			}
			continue
		}

		successfulCopies = append(successfulCopies, element)
	}
	return successfulCopies
}

func deleteOldObjects(sess *session.Session, sourceBucket *string, keys []*string) []*string {
	svc := s3.New(sess)
	var successfulDeletes []*string
	for _, element := range keys {
		input := &s3.DeleteObjectInput{
			Bucket: aws.String(*sourceBucket),
			Key:    aws.String(*element),
		}

		_, err := svc.DeleteObject(input)
		if err != nil {
			if aerr, ok := err.(awserr.Error); ok {
				switch aerr.Code() {
				default:
					fmt.Println(aerr.Error())
				}
			} else {
				// Print the error, cast err to awserr.Error to get the Code and
				// Message from an error.
				fmt.Println(err.Error())
			}
			continue
		}

		successfulDeletes = append(successfulDeletes, element)
	}
	return successfulDeletes
}
