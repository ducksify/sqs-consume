package consumer

import (
	"context"
	"errors"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

const (
	DefaultMaxNumberOfMessages = int32(10)
	DefaultWaitTimeSeconds     = int32(5)
	DefaultConcurrency         = 5

	DeleteStrategyImmediate = DeleteStrategy("IMMEDIATE")
	DeleteStrategyOnSuccess = DeleteStrategy("ON_SUCCESS")
)

var (
	SentinelErrorQueueNotSet = errors.New("queue not set")
	SentinelErrorConfigIsNil = errors.New("configuration is nil")
	SentinelErrorConfigAws   = errors.New("aws configuration error")
)

type DeleteStrategy string

type SQSConf struct {
	Queue               string
	Concurrency         int
	MaxNumberOfMessages int32
	VisibilityTimeout   int32
	WaitTimeSeconds     int32
	DeleteStrategy      DeleteStrategy
}

type SQSClient interface {
	ReceiveMessage(ctx context.Context, params *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error)
	DeleteMessageBatch(ctx context.Context, params *sqs.DeleteMessageBatchInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageBatchOutput, error)
}

type SQS struct {
	config *SQSConf
	sqs    SQSClient
}

type ConsumerFn func(data []byte, attributes map[string]types.MessageAttributeValue) error
