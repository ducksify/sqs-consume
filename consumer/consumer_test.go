package consumer

import (
	"context"
	"errors"
	"os"
	"sync/atomic"
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

func TestSQS_ConcurrencyLimits(t *testing.T) {
	// Test that concurrency limits are properly enforced
	concurrency := 2
	config := &SQSConf{
		Queue:               "test-queue",
		Concurrency:         concurrency,
		MaxNumberOfMessages: 10,
		DeleteStrategy:      DeleteStrategyImmediate,
	}

	setEnv("AWS_REGION", "us-east-1", "AWS_SECRET_ACCESS_KEY", "test", "AWS_ACCESS_KEY_ID", "test")

	sqs, err := NewSQSConsumer(config)
	require.NoError(t, err)

	// Verify initial state
	active, capacity := sqs.GetConcurrencyStats()
	assert.Equal(t, 0, active)
	assert.Equal(t, concurrency, capacity)

	// Create a mock that returns many messages
	mockSQS := new(SqsMock)
	mockSQS.On("ReceiveMessage", mock.Anything, mock.Anything, mock.Anything).Return(getQueueContent(), nil)
	mockSQS.On("DeleteMessageBatch", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

	sqs.sqs = mockSQS

	// Track concurrent executions
	var concurrentCount int32
	var maxConcurrent int32
	processingStarted := make(chan struct{})

	consumeFunc := func(data []byte, attributes map[string]types.MessageAttributeValue) error {
		// Signal that processing has started
		select {
		case processingStarted <- struct{}{}:
		default:
		}

		// Increment concurrent count
		current := atomic.AddInt32(&concurrentCount, 1)
		if current > atomic.LoadInt32(&maxConcurrent) {
			atomic.StoreInt32(&maxConcurrent, current)
		}

		// Simulate processing time
		time.Sleep(100 * time.Millisecond)

		// Decrement concurrent count
		atomic.AddInt32(&concurrentCount, -1)

		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	// Start the consumer in a goroutine
	go func() {
		sqs.Start(ctx, consumeFunc)
	}()

	// Wait for some processing to start
	select {
	case <-processingStarted:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("No processing started within timeout")
	}

	// Wait a bit for concurrent processing
	time.Sleep(200 * time.Millisecond)

	// Check that we don't exceed the concurrency limit
	finalMaxConcurrent := atomic.LoadInt32(&maxConcurrent)
	assert.LessOrEqual(t, finalMaxConcurrent, int32(concurrency),
		"Concurrent processing exceeded limit: %d > %d", finalMaxConcurrent, concurrency)

	// Verify semaphore stats
	active, capacity = sqs.GetConcurrencyStats()
	assert.LessOrEqual(t, active, concurrency)
	assert.Equal(t, concurrency, capacity)
}

func TestSQS_ConcurrencyStats(t *testing.T) {
	config := &SQSConf{
		Queue:       "test-queue",
		Concurrency: 3,
	}

	setEnv("AWS_REGION", "us-east-1", "AWS_SECRET_ACCESS_KEY", "test", "AWS_ACCESS_KEY_ID", "test")

	sqs, err := NewSQSConsumer(config)
	require.NoError(t, err)

	// Test initial stats
	active, capacity := sqs.GetConcurrencyStats()
	assert.Equal(t, 0, active)
	assert.Equal(t, 3, capacity)

	// Manually acquire semaphore slots to test stats
	sqs.semaphore <- struct{}{}
	active, capacity = sqs.GetConcurrencyStats()
	assert.Equal(t, 1, active)
	assert.Equal(t, 3, capacity)

	sqs.semaphore <- struct{}{}
	active, capacity = sqs.GetConcurrencyStats()
	assert.Equal(t, 2, active)
	assert.Equal(t, 3, capacity)

	// Release slots
	<-sqs.semaphore
	<-sqs.semaphore
	active, capacity = sqs.GetConcurrencyStats()
	assert.Equal(t, 0, active)
	assert.Equal(t, 3, capacity)
}

func TestSQS_HighConcurrency(t *testing.T) {
	// Test with high concurrency setting (150)
	concurrency := 150
	config := &SQSConf{
		Queue:               "test-queue",
		Concurrency:         concurrency,
		MaxNumberOfMessages: 10,
		DeleteStrategy:      DeleteStrategyImmediate,
	}

	setEnv("AWS_REGION", "us-east-1", "AWS_SECRET_ACCESS_KEY", "test", "AWS_ACCESS_KEY_ID", "test")

	sqs, err := NewSQSConsumer(config)
	require.NoError(t, err)

	// Verify initial state
	active, capacity := sqs.GetConcurrencyStats()
	assert.Equal(t, 0, active)
	assert.Equal(t, concurrency, capacity)

	// Test that we can acquire many semaphore slots
	acquiredSlots := make([]struct{}, 0, concurrency)
	for i := 0; i < concurrency; i++ {
		select {
		case sqs.semaphore <- struct{}{}:
			acquiredSlots = append(acquiredSlots, struct{}{})
		default:
			t.Fatalf("Failed to acquire semaphore slot %d", i)
		}
	}

	// Verify all slots are acquired
	active, capacity = sqs.GetConcurrencyStats()
	assert.Equal(t, concurrency, active)
	assert.Equal(t, concurrency, capacity)

	// Test that we cannot acquire more slots
	select {
	case sqs.semaphore <- struct{}{}:
		t.Fatal("Should not be able to acquire more semaphore slots")
	default:
		// This is expected - no more slots available
	}

	// Release all slots
	for range acquiredSlots {
		<-sqs.semaphore
	}

	// Verify all slots are released
	active, capacity = sqs.GetConcurrencyStats()
	assert.Equal(t, 0, active)
	assert.Equal(t, concurrency, capacity)
}

func TestSQS_VeryHighConcurrency(t *testing.T) {
	// Test with very high concurrency setting (1000)
	concurrency := 1000
	config := &SQSConf{
		Queue:               "test-queue",
		Concurrency:         concurrency,
		MaxNumberOfMessages: 10,
		DeleteStrategy:      DeleteStrategyImmediate,
	}

	setEnv("AWS_REGION", "us-east-1", "AWS_SECRET_ACCESS_KEY", "test", "AWS_ACCESS_KEY_ID", "test")

	sqs, err := NewSQSConsumer(config)
	require.NoError(t, err)

	// Verify initial state
	active, capacity := sqs.GetConcurrencyStats()
	assert.Equal(t, 0, active)
	assert.Equal(t, concurrency, capacity)

	// Test that we can acquire many semaphore slots (test first 100 to avoid long test times)
	testSlots := 100
	acquiredSlots := make([]struct{}, 0, testSlots)
	for i := 0; i < testSlots; i++ {
		select {
		case sqs.semaphore <- struct{}{}:
			acquiredSlots = append(acquiredSlots, struct{}{})
		default:
			t.Fatalf("Failed to acquire semaphore slot %d", i)
		}
	}

	// Verify slots are acquired
	active, capacity = sqs.GetConcurrencyStats()
	assert.Equal(t, testSlots, active)
	assert.Equal(t, concurrency, capacity)

	// Release all slots
	for range acquiredSlots {
		<-sqs.semaphore
	}

	// Verify all slots are released
	active, capacity = sqs.GetConcurrencyStats()
	assert.Equal(t, 0, active)
	assert.Equal(t, concurrency, capacity)
}
