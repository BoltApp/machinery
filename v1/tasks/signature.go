package tasks

import (
	"fmt"
	"net/http"
	"time"

	"github.com/RichardKnop/machinery/v1/utils"

	"github.com/google/uuid"
)

// Arg represents a single argument passed to invocation fo a task
type Arg struct {
	Name  string      `bson:"name"`
	Type  string      `bson:"type"`
	Value interface{} `bson:"value"`
}

// Headers represents the headers which should be used to direct the task
// Deprecated: Do not use DeprecatedSignature over Signature
type Headers map[string]interface{}

// Set on Headers implements opentracing.TextMapWriter for trace propagation
func (h Headers) Set(key, val string) {
	h[key] = val
}

// ForeachKey on Headers implements opentracing.TextMapReader for trace propagation.
// It is essentially the same as the opentracing.TextMapReader implementation except
// for the added casting from interface{} to string.
func (h Headers) ForeachKey(handler func(key, val string) error) error {
	for k, v := range h {
		// Skip any non string values
		stringValue, ok := v.(string)
		if !ok {
			continue
		}

		if err := handler(k, stringValue); err != nil {
			return err
		}
	}

	return nil
}

// DeprecatedSignature represents a single task invocation
// Deprecated: This supports the previous iteration of Signature that uses Headers over http.Header
type DeprecatedSignature struct {
	UUID           string
	Name           string
	RoutingKey     string
	ETA            *time.Time
	GroupUUID      string
	GroupTaskCount int
	Args           []Arg
	Headers        Headers
	Priority       uint8
	Immutable      bool
	RetryCount     int
	RetryTimeout   int
	OnSuccess      []*Signature
	OnError        []*Signature
	ChordCallback  *Signature
	//MessageGroupId for Broker, e.g. SQS
	BrokerMessageGroupId string
	//ReceiptHandle of SQS Message
	SQSReceiptHandle string
	// StopTaskDeletionOnError used with sqs when we want to send failed messages to dlq,
	// and don't want machinery to delete from source queue
	StopTaskDeletionOnError bool
	// IgnoreWhenTaskNotRegistered auto removes the request when there is no handeler available
	// When this is true a task with no handler will be ignored and not placed back in the queue
	IgnoreWhenTaskNotRegistered bool
}

// Signature represents a single task invocation
type Signature struct {
	UUID           string
	Name           string
	RoutingKey     string
	ETA            *time.Time
	GroupUUID      string
	GroupTaskCount int
	Args           []Arg
	Headers        http.Header
	Priority       uint8
	Immutable      bool
	RetryCount     int
	RetryTimeout   int
	OnSuccess      []*Signature
	OnError        []*Signature
	ChordCallback  *Signature
	//MessageGroupId for Broker, e.g. SQS
	BrokerMessageGroupId string
	//ReceiptHandle of SQS Message
	SQSReceiptHandle string
	// StopTaskDeletionOnError used with sqs when we want to send failed messages to dlq,
	// and don't want machinery to delete from source queue
	StopTaskDeletionOnError bool
	// IgnoreWhenTaskNotRegistered auto removes the request when there is no handeler available
	// When this is true a task with no handler will be ignored and not placed back in the queue
	IgnoreWhenTaskNotRegistered bool
}

// NewSignature creates a new task signature
func NewSignature(name string, args []Arg) (*Signature, error) {
	signatureID := uuid.New().String()
	return &Signature{
		UUID: fmt.Sprintf("task_%v", signatureID),
		Name: name,
		Args: args,
	}, nil
}

func CopySignatures(signatures ...*Signature) []*Signature {
	var sigs = make([]*Signature, len(signatures))
	for index, signature := range signatures {
		sigs[index] = CopySignature(signature)
	}
	return sigs
}

func CopySignature(signature *Signature) *Signature {
	var sig = new(Signature)
	_ = utils.DeepCopy(sig, signature)
	return sig
}

func (d *DeprecatedSignature) ConvertToSignature() (*Signature, error) {
	convertedHeader, err := convertHeaders(d.Headers)
	if err != nil {
		return nil, err
	}

	signature := &Signature{
		UUID:                        d.UUID,
		Name:                        d.Name,
		RoutingKey:                  d.RoutingKey,
		ETA:                         d.ETA,
		GroupUUID:                   d.GroupUUID,
		GroupTaskCount:              d.GroupTaskCount,
		Args:                        d.Args,
		Headers:                     convertedHeader,
		Priority:                    d.Priority,
		Immutable:                   d.Immutable,
		RetryCount:                  d.RetryCount,
		RetryTimeout:                d.RetryTimeout,
		OnSuccess:                   d.OnSuccess,
		OnError:                     d.OnError,
		ChordCallback:               d.ChordCallback,
		BrokerMessageGroupId:        d.BrokerMessageGroupId,
		SQSReceiptHandle:            d.SQSReceiptHandle,
		StopTaskDeletionOnError:     d.StopTaskDeletionOnError,
		IgnoreWhenTaskNotRegistered: d.IgnoreWhenTaskNotRegistered,
	}
	return signature, nil
}

func convertHeaders(headers Headers) (http.Header, error) {
	httpHeader := http.Header{}
	err := headers.ForeachKey(func(key, val string) error {
		httpHeader.Set(key, val)
		return nil
	})
	if err != nil {
		return nil, err
	}

	return httpHeader, nil
}
