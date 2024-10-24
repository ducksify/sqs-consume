package consumer

import (
	"context"
	"errors"
	"github.com/aws/aws-sdk-go-v2/aws"
	_ "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/stretchr/testify/assert"
	_ "github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
	"time"
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
	// Map to store original values to restore them later
	// Loop through the provided key-value pairs
	for i := 0; i < len(keyValue); i += 2 {
		key := keyValue[i]
		value := keyValue[i+1]
		os.Setenv(key, value)
	}
}
func unsetEnv(keyValue ...string) {
	// Map to store original values to restore them later
	// Loop through the provided key-value pairs
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

	type args struct {
		conf      *SQSConf
		consumeFn ConsumerFn
		env       []string
	}
	tests := []struct {
		name    string
		args    args
		want    *SQS
		wantErr error
	}{
		{
			name: "shouldCreateNewSQSConsumer",
			args: args{
				conf:      sqsConf,
				consumeFn: consumeTestFunc,
				env:       []string{"AWS_REGION", "baz", "AWS_SECRET_ACCESS_KEY", "foo", "AWS_ACCESS_KEY_ID", "bar"},
			},
			want: &SQS{
				config: sqsConf,
				sqs:    svc,
			},

			wantErr: nil,
		},
		{
			name: "shouldCreateNewSQSConsumerWithDefaultValues",
			args: args{
				conf: &SQSConf{
					Queue: "queue",
				},
				env:       []string{"AWS_REGION", "baz", "AWS_SECRET_ACCESS_KEY", "foo", "AWS_ACCESS_KEY_ID", "bar"},
				consumeFn: consumeTestFunc,
			},
			want: &SQS{
				config: &SQSConf{
					Queue:               "queue",
					Concurrency:         DefaultConcurrency,
					MaxNumberOfMessages: DefaultMaxNumberOfMessages,
					WaitTimeSeconds:     DefaultWaitTimeSeconds,
					DeleteStrategy:      DeleteStrategyImmediate,
				},
				sqs: svc,
			},

			wantErr: nil,
		},
		{
			name: "shouldErrorQueueEmptyNewSQSConsumer",
			args: args{
				conf: &SQSConf{
					Queue: "",
				},
				env:       []string{"AWS_REGION", "baz", "AWS_SECRET_ACCESS_KEY", "foo", "AWS_ACCESS_KEY_ID", "bar"},
				consumeFn: consumeTestFunc,
			},
			want: &SQS{
				config: sqsConf,
				sqs:    svc,
			},

			wantErr: SentinelErrorQueueNotSet,
		},
		{
			name: "shouldErrorConfigNilNewSQSConsumer",
			args: args{
				conf:      nil,
				env:       []string{"AWS_REGION", "baz", "AWS_SECRET_ACCESS_KEY", "foo", "AWS_ACCESS_KEY_ID", "bar"},
				consumeFn: consumeTestFunc,
			},
			want: &SQS{
				config: sqsConf,
				sqs:    svc,
			},

			wantErr: SentinelErrorConfigIsNil,
		},
		{
			name: "shouldErrorMissingEnv",
			args: args{
				conf:      nil,
				env:       []string{"AWS_SECRET_ACCESS_KEY", "foo", "AWS_ACCESS_KEY_ID", "bar"},
				consumeFn: consumeTestFunc,
			},
			want: &SQS{
				config: sqsConf,
				sqs:    svc,
			},

			wantErr: SentinelErrorConfigAws,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// set Env
			unsetEnv("AWS_REGION", "baz", "AWS_SECRET_ACCESS_KEY", "foo", "AWS_ACCESS_KEY_ID", "bar")

			setEnv(tt.args.env...)

			got, err := NewSQSConsumer(tt.args.conf)
			if err != nil && tt.wantErr == nil {
				t.Errorf("Error creation new Consumer")
				return
			}
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				return
			}
			require.Equal(t, tt.want.config, got.config)
			unsetEnv(tt.args.env...)
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

	type fields struct {
		config *SQSConf
		svc    SQSClient
	}

	type args struct {
		consumeFn ConsumerFn
	}
	var tests = []struct {
		name           string
		fields         fields
		args           args
		wantReceiveErr error
		wantDeleteErr  error
	}{
		{
			name: "shouldHandleMessage",
			fields: fields{
				config: &SQSConf{
					Queue:          queueUrl,
					DeleteStrategy: DeleteStrategyImmediate,
				},
				svc: new(SqsMock),
			},
			args: args{
				consumeFn: consumeTestFunc,
			},
			wantReceiveErr: nil,
			wantDeleteErr:  nil,
		},
		///*
		{
			name: "should error when receive",
			fields: fields{
				config: &SQSConf{
					Queue:          queueUrl,
					DeleteStrategy: DeleteStrategyImmediate,
				},
				svc: new(SqsMock),
			},
			args: args{
				consumeFn: consumeTestFunc,
			},
			wantReceiveErr: errors.New("fake receive error"),
			wantDeleteErr:  nil,
		},
		{
			name: "should error when delete",
			fields: fields{
				config: &SQSConf{
					Queue:          queueUrl,
					DeleteStrategy: DeleteStrategyImmediate,
				},
				svc: new(SqsMock),
			},
			args: args{
				consumeFn: consumeTestFunc,
			},
			wantReceiveErr: nil,
			wantDeleteErr:  errors.New("fake delete error"),
		},
		{
			name: "should context timeout",
			fields: fields{
				config: &SQSConf{
					Queue:          queueUrl,
					DeleteStrategy: DeleteStrategyImmediate,
				},
				svc: new(SqsMock),
			},
			args: args{
				consumeFn: consumeTestFunc,
			},
			wantReceiveErr: nil,
			wantDeleteErr:  nil,
		},

		//*/
	}
	for _, tt := range tests {
		actualData = make([]string, 0)
		t.Run(tt.name, func(t *testing.T) {
			sqsMock := tt.fields.svc.(*SqsMock)
			sqsMock.On("ReceiveMessage", mock.Anything, mock.AnythingOfType("*sqs.ReceiveMessageInput"),
				mock.AnythingOfType("[]func(*sqs.Options)")).Return(getQueueContent(), tt.wantReceiveErr)
			sqsMock.On("DeleteMessageBatch", mock.Anything, mock.AnythingOfType("*sqs.DeleteMessageBatchInput"),
				mock.AnythingOfType("[]func(*sqs.Options)")).Return(nil, tt.wantDeleteErr)

			ctx, _ := context.WithTimeout(context.Background(), 50*time.Millisecond)

			// set Env
			setEnv("AWS_REGION", "baz", "AWS_SECRET_ACCESS_KEY", "foo", "AWS_ACCESS_KEY_ID", "bar")

			s, err := NewSQSConsumer(tt.fields.config)
			if err != nil {
				t.Errorf("Error creation new Consumer")
				return
			}

			s.sqs = sqsMock
			err = s.Start(ctx, tt.args.consumeFn)

			t.Log(len(actualData))
			t.Log(len(actualAttributes))
			mocked := tt.fields.svc.(*SqsMock)
			if tt.wantReceiveErr == nil && tt.wantDeleteErr == nil {
				assert.NotNil(t, mocked.inputs)
				assert.NotNil(t, mocked.deleteInputs)
				for _, msg := range actualData {
					assert.Contains(t, []string{
						"msg1",
						"msg2",
						"msg3",
					}, msg)
				}
				for _, attr := range actualAttributes {
					for attrKey := range attr {
						assert.Contains(t, []string{
							"attribute1",
							"attribute2",
							"attribute3",
						}, attrKey)
					}
				}
			} else {
				assert.NotNil(t, err)
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
					"attribute1": {
						DataType:    aws.String("String"),
						StringValue: aws.String("foo"),
					},
				},
			},
			{
				MessageId: aws.String("msg2"),
				Body:      aws.String("msg2"),
				MessageAttributes: map[string]types.MessageAttributeValue{
					"attribute2": {
						DataType:    aws.String("String"),
						StringValue: aws.String("foo"),
					},
				},
			},
			{
				MessageId: aws.String("msg3"),
				Body:      aws.String("msg3"),
				MessageAttributes: map[string]types.MessageAttributeValue{
					"attribute3": {
						DataType:    aws.String("String"),
						StringValue: aws.String("foo"),
					},
				},
			},
		},
	}

}
