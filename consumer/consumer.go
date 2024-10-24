package consumer

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"golang.org/x/sync/errgroup"
	"log/slog"
	"os"
	"os/signal"
	"time"
)

func NewSQSConsumer(conf *SQSConf) (*SQS, error) {

	if os.Getenv("AWS_ACCESS_KEY_ID") == "" || os.Getenv("AWS_SECRET_ACCESS_KEY") == "" || os.Getenv("AWS_REGION") == "" {
		slog.Error("One or more AWS environment variables are not set.")
		return nil, SentinelErrorConfigAws
	}
	cred := credentials.NewStaticCredentialsProvider(os.Getenv("AWS_ACCESS_KEY_ID"), os.Getenv("AWS_SECRET_ACCESS_KEY"), "")

	awsCfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion(os.Getenv("AWS_REGION")),
		config.WithCredentialsProvider(cred),
	)

	if err != nil {
		slog.Error("Error creation AWS configuration.")
		return nil, SentinelErrorConfigAws
	}
	sqsClient := sqs.NewFromConfig(awsCfg)

	if conf == nil {
		return nil, SentinelErrorConfigIsNil
	}

	if conf.Queue == "" {
		return nil, SentinelErrorQueueNotSet
	}

	if len(conf.DeleteStrategy) == 0 {
		conf.DeleteStrategy = DeleteStrategyImmediate
	}

	if conf.Concurrency == 0 {
		conf.Concurrency = DefaultConcurrency
	}

	if conf.WaitTimeSeconds == 0 {
		conf.WaitTimeSeconds = DefaultWaitTimeSeconds
	}

	if conf.MaxNumberOfMessages == 0 {
		conf.MaxNumberOfMessages = DefaultMaxNumberOfMessages
	}

	return &SQS{config: conf, sqs: sqsClient}, nil
}

func (s *SQS) Start(ctx context.Context, consumeFn ConsumerFn) error {
	ctx, cancel := context.WithCancel(ctx)

	go func() {
		c := make(chan os.Signal)
		signal.Notify(c, os.Interrupt)
		_ = <-c
		cancel()
	}()

	g, ctx := errgroup.WithContext(ctx)

	for i := 0; i < s.config.Concurrency; i++ {
		g.Go(func() error {
			return s.handleMessages(ctx, consumeFn)
		})
	}

	return g.Wait()
}

func (s *SQS) handleMessages(ctx context.Context, consumeFn ConsumerFn) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			result, err := s.sqs.ReceiveMessage(ctx, s.pullMessagesRequest())

			if err != nil {
				return err
			}

			if len(result.Messages) == 0 {
				time.Sleep(1 * time.Second)
				continue
			}

			if s.config.DeleteStrategy == DeleteStrategyImmediate {
				if err := s.deleteSqsMessages(ctx, result.Messages); err != nil {
					return err
				}
			}

			toDelete := make([]types.Message, 0)
			for _, msg := range result.Messages {
				if err := consumeFn([]byte(*msg.Body), msg.MessageAttributes); err != nil {
					slog.Error("error in consume function", slog.Any("error", err.Error()))
					continue
				}

				if s.config.DeleteStrategy == DeleteStrategyOnSuccess {
					toDelete = append(toDelete, msg)
				}
			}

			if err := s.deleteSqsMessages(ctx, toDelete); err != nil {
				return err
			}

		}
	}
}

func (s *SQS) pullMessagesRequest() *sqs.ReceiveMessageInput {

	r := &sqs.ReceiveMessageInput{
		MessageSystemAttributeNames: []types.MessageSystemAttributeName{types.MessageSystemAttributeNameAll},
		MessageAttributeNames: []string{
			string(types.MessageSystemAttributeNameSentTimestamp),
		},
		QueueUrl:            aws.String(s.config.Queue),
		MaxNumberOfMessages: s.config.MaxNumberOfMessages,
		VisibilityTimeout:   s.config.VisibilityTimeout,
		WaitTimeSeconds:     s.config.WaitTimeSeconds,
	}
	return r
}

func (s *SQS) deleteSqsMessages(ctx context.Context, msg []types.Message) error {
	if len(msg) == 0 {
		return nil
	}

	chunks := chunk(msg, 10) // max batch size for SQS is 10

	for _, chunk := range chunks {
		batch := make([]types.DeleteMessageBatchRequestEntry, len(chunk))

		for i, v := range chunk {
			batch[i] = types.DeleteMessageBatchRequestEntry{
				Id:            aws.String(*v.MessageId),
				ReceiptHandle: v.ReceiptHandle,
			}
		}

		_, err := s.sqs.DeleteMessageBatch(ctx, &sqs.DeleteMessageBatchInput{
			Entries:  batch,
			QueueUrl: aws.String(s.config.Queue),
		})

		if err != nil {
			return err
		}
	}

	return nil

}

func chunk(rows []types.Message, chunkSize int) [][]types.Message {
	var chunk []types.Message
	chunks := make([][]types.Message, 0, len(rows)/chunkSize+1)

	for len(rows) >= chunkSize {
		chunk, rows = rows[:chunkSize], rows[chunkSize:]
		chunks = append(chunks, chunk)
	}

	if len(rows) > 0 {
		chunks = append(chunks, rows[:])
	}

	return chunks
}
