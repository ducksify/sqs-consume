package consumer

import (
	"context"
	"log/slog"
	"os"
	"os/signal"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
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
		slog.Error("Error creating AWS configuration.")
		return nil, SentinelErrorConfigAws
	}

	sqsClient := sqs.NewFromConfig(awsCfg)

	if conf == nil {
		return nil, SentinelErrorConfigIsNil
	}

	if conf.Queue == "" {
		return nil, SentinelErrorQueueNotSet
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

	return &SQS{
		config:    conf,
		sqs:       sqsClient,
		semaphore: make(chan struct{}, conf.Concurrency),
		slotFree:  make(chan struct{}, 1),
	}, nil
}

func (s *SQS) releaseSlot() {
	<-s.semaphore
	select {
	case s.slotFree <- struct{}{}:
	default:
	}
}

func (s *SQS) Start(ctx context.Context, consumeFn ConsumerFn) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Graceful shutdown
	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt)
		<-c
		cancel()
	}()

	for {
		select {
		case <-ctx.Done():
			return nil

		default:
			availableSlots := cap(s.semaphore) - len(s.semaphore)
			if availableSlots > 0 {
				slog.Info("Polling messages from SQS, available slots : ", slog.Int("availableSlots", availableSlots))
				result, err := s.sqs.ReceiveMessage(ctx, s.pullMessagesRequest(availableSlots))
				if err != nil {
					return err
				}
				slog.Info("Received messages from SQS : ", slog.Int("messages", len(result.Messages)))

				for _, msg := range result.Messages {
					select {
					case s.semaphore <- struct{}{}:
						go func(m types.Message) {
							defer s.releaseSlot()

							if err := s.deleteSqsMessages(ctx, []types.Message{m}); err != nil {
								slog.Error("failed to delete message", slog.Any("error", err.Error()))
							}

							consumeFn([]byte(*m.Body), m.MessageAttributes)
						}(msg)
					case <-ctx.Done():
						return nil
					default:
						slog.Warn("SHOULD NOT HAPPEN ! semaphore became full, skipping remaining messages", slog.Int("skipped", len(result.Messages)-1))
					}
				}
			} else {
				select {
				case <-s.slotFree:
				case <-ctx.Done():
					return nil
				}
			}
		}
	}
}

func (s *SQS) pullMessagesRequest(availableSlots int) *sqs.ReceiveMessageInput {
	maxMessages := s.config.MaxNumberOfMessages
	if slots := int32(availableSlots); slots < maxMessages {
		maxMessages = slots
	}
	return &sqs.ReceiveMessageInput{
		MessageSystemAttributeNames: []types.MessageSystemAttributeName{types.MessageSystemAttributeNameAll},
		MessageAttributeNames:       []string{"All"},
		QueueUrl:                    aws.String(s.config.Queue),
		MaxNumberOfMessages:         maxMessages,
		VisibilityTimeout:           s.config.VisibilityTimeout,
		WaitTimeSeconds:             s.config.WaitTimeSeconds,
	}
}

func (s *SQS) deleteSqsMessages(ctx context.Context, msgs []types.Message) error {
	if len(msgs) == 0 {
		return nil
	}

	chunks := chunk(msgs, 10)
	for _, chunk := range chunks {
		batch := make([]types.DeleteMessageBatchRequestEntry, len(chunk))
		for i, m := range chunk {
			batch[i] = types.DeleteMessageBatchRequestEntry{
				Id:            aws.String(*m.MessageId),
				ReceiptHandle: m.ReceiptHandle,
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

func chunk(msgs []types.Message, chunkSize int) [][]types.Message {
	var chunks [][]types.Message
	for len(msgs) > chunkSize {
		chunks = append(chunks, msgs[:chunkSize])
		msgs = msgs[chunkSize:]
	}
	if len(msgs) > 0 {
		chunks = append(chunks, msgs)
	}
	return chunks
}
