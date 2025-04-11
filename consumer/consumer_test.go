package consumer

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type SqsMock struct {
	mock.Mock
	inputs       []*sqs.ReceiveMessageInput
	receiveError error
	deleteInputs []*sqs.DeleteMessageBatchInput
	deleteError  error
}

func (m *SqsMock) ReceiveMessage(ctx context.Context, params *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	args := m.Called(ctx, params, optFns)
	m.inputs = append(m.inputs, params)
	return getQueueContent(), args.Error(1)
}

func (m *SqsMock) DeleteMessageBatch(ctx context.Context, params *sqs.DeleteMessageBatchInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageBatchOutput, error) {
	args := m.Called(ctx, params, optFns)
	m.deleteInputs = append(m.deleteInputs, params)
	return nil, args.Error(1)
}

func setEnv(keyValue ...string) {
	for i := 0; i < len(keyValue); i += 2 {
		key := keyValue[i]
		value := keyValue[i+1]
		os.Setenv(key, value)
	}
}
func unsetEnv(keyValue ...string) {
	for i := 0; i < len(keyValue); i += 2 {
		key := keyValue[i]
		os.Unsetenv(key)
	}
}

func TestNewSQSWorker(t *testing.T) {
	sqsConf := &SQSConf{
		Queue:               "queue",
		Concurrency:         2,
		MaxNumberOfMessages: 10,
		VisibilityTimeout:   30,
		WaitTimeSeconds:     20,
	}

	cfg, _ := config.LoadDefaultConfig(context.TODO(),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("", "", "")),
		config.WithRegion(""),
	)

	svc := sqs.NewFromConfig(cfg)

	tests := []struct {
		name    string
		conf    *SQSConf
		env     []string
		want    *SQS
		wantErr error
	}{
		{
			name: "shouldCreateNewSQSConsumer",
			conf: sqsConf,
			env:  []string{"AWS_REGION", "baz", "AWS_SECRET_ACCESS_KEY", "foo", "AWS_ACCESS_KEY_ID", "bar"},
			want: &SQS{
				config:    sqsConf,
				sqs:       svc,
				semaphore: make(chan struct{}, sqsConf.Concurrency),
			},
			wantErr: nil,
		},
		{
			name: "shouldCreateNewSQSConsumerWithDefaultValues",
			conf: &SQSConf{
				Queue: "queue",
			},
			env: []string{"AWS_REGION", "baz", "AWS_SECRET_ACCESS_KEY", "foo", "AWS_ACCESS_KEY_ID", "bar"},
			want: &SQS{
				config: &SQSConf{
					Queue:               "queue",
					Concurrency:         DefaultConcurrency,
					MaxNumberOfMessages: DefaultMaxNumberOfMessages,
					WaitTimeSeconds:     DefaultWaitTimeSeconds,
					DeleteStrategy:      DeleteStrategyImmediate,
				},
				sqs:       svc,
				semaphore: make(chan struct{}, DefaultConcurrency),
			},
			wantErr: nil,
		},
		{
			name:    "shouldErrorQueueEmptyNewSQSConsumer",
			conf:    &SQSConf{Queue: ""},
			env:     []string{"AWS_REGION", "baz", "AWS_SECRET_ACCESS_KEY", "foo", "AWS_ACCESS_KEY_ID", "bar"},
			wantErr: SentinelErrorQueueNotSet,
		},
		{
			name:    "shouldErrorConfigNilNewSQSConsumer",
			conf:    nil,
			env:     []string{"AWS_REGION", "baz", "AWS_SECRET_ACCESS_KEY", "foo", "AWS_ACCESS_KEY_ID", "bar"},
			wantErr: SentinelErrorConfigIsNil,
		},
		{
			name:    "shouldErrorMissingEnv",
			conf:    &SQSConf{Queue: "queue"},
			env:     []string{"AWS_SECRET_ACCESS_KEY", "foo", "AWS_ACCESS_KEY_ID", "bar"},
			wantErr: SentinelErrorConfigAws,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			unsetEnv("AWS_REGION", "AWS_SECRET_ACCESS_KEY", "AWS_ACCESS_KEY_ID")
			setEnv(tt.env...)

			got, err := NewSQSConsumer(tt.conf)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want.config, got.config)
			require.Equal(t, cap(tt.want.semaphore), cap(got.semaphore))
		})
	}
}

var consumeTestFunc ConsumerFn

func TestSQS_Start(t *testing.T) {
	queueUrl := "queue"
	var actualData []string
	actualAttributes := make([]map[string]types.MessageAttributeValue, 0)

	consumeTestFunc = func(data []byte, attributes map[string]types.MessageAttributeValue) error {
		actualData = append(actualData, string(data))
		actualAttributes = append(actualAttributes, attributes)
		return nil
	}

	tests := []struct {
		name           string
		config         *SQSConf
		wantReceiveErr error
		wantDeleteErr  error
	}{
		{
			name: "shouldHandleMessage",
			config: &SQSConf{
				Queue:          queueUrl,
				DeleteStrategy: DeleteStrategyImmediate,
			},
			wantReceiveErr: nil,
			wantDeleteErr:  nil,
		},
		{
			name: "should error when receive",
			config: &SQSConf{
				Queue:          queueUrl,
				DeleteStrategy: DeleteStrategyImmediate,
			},
			wantReceiveErr: errors.New("fake receive error"),
			wantDeleteErr:  nil,
		},
		{
			name: "should error when delete",
			config: &SQSConf{
				Queue:          queueUrl,
				DeleteStrategy: DeleteStrategyImmediate,
			},
			wantReceiveErr: nil,
			wantDeleteErr:  errors.New("fake delete error"),
		},
		{
			name: "should context timeout",
			config: &SQSConf{
				Queue:          queueUrl,
				DeleteStrategy: DeleteStrategyImmediate,
			},
			wantReceiveErr: nil,
			wantDeleteErr:  nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actualData = make([]string, 0)
			actualAttributes = make([]map[string]types.MessageAttributeValue, 0)

			mockSQS := new(SqsMock)
			mockSQS.On("ReceiveMessage", mock.Anything, mock.Anything, mock.Anything).Return(getQueueContent(), tt.wantReceiveErr)
			mockSQS.On("DeleteMessageBatch", mock.Anything, mock.Anything, mock.Anything).Return(nil, tt.wantDeleteErr)

			setEnv("AWS_REGION", "baz", "AWS_SECRET_ACCESS_KEY", "foo", "AWS_ACCESS_KEY_ID", "bar")

			s, err := NewSQSConsumer(tt.config)
			require.NoError(t, err)
			s.sqs = mockSQS

			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			err = s.Start(ctx, consumeTestFunc)

			if tt.wantReceiveErr != nil {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.NotEmpty(t, actualData)
				for _, msg := range actualData {
					assert.Contains(t, []string{"msg1", "msg2", "msg3"}, msg)
				}
			}
		})
	}
}

func getQueueContent() *sqs.ReceiveMessageOutput {
	return &sqs.ReceiveMessageOutput{
		Messages: []types.Message{
			{
				MessageId: aws.String("msg1"),
				Body:      aws.String("msg1"),
				MessageAttributes: map[string]types.MessageAttributeValue{
					"attribute1": {DataType: aws.String("String"), StringValue: aws.String("foo")},
				},
			},
			{
				MessageId: aws.String("msg2"),
				Body:      aws.String("msg2"),
				MessageAttributes: map[string]types.MessageAttributeValue{
					"attribute2": {DataType: aws.String("String"), StringValue: aws.String("foo")},
				},
			},
			{
				MessageId: aws.String("msg3"),
				Body:      aws.String("msg3"),
				MessageAttributes: map[string]types.MessageAttributeValue{
					"attribute3": {DataType: aws.String("String"), StringValue: aws.String("foo")},
				},
			},
		},
	}
}
