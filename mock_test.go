package kafka

import (
	"context"
	"testing"

	"github.com/IBM/sarama"
)

type fakeSyncProducer struct{}

func (f *fakeSyncProducer) SendMessage(msg *sarama.ProducerMessage) (int32, int64, error) {
	return 0, 0, nil
}
func (f *fakeSyncProducer) SendMessages(msgs []*sarama.ProducerMessage) error { return nil }
func (f *fakeSyncProducer) Close() error                                      { return nil }
func (f *fakeSyncProducer) TxnStatus() sarama.ProducerTxnStatusFlag           { return 0 }
func (f *fakeSyncProducer) IsTransactional() bool                             { return false }
func (f *fakeSyncProducer) BeginTxn() error                                   { return nil }
func (f *fakeSyncProducer) CommitTxn() error                                  { return nil }
func (f *fakeSyncProducer) AbortTxn() error                                   { return nil }
func (f *fakeSyncProducer) AddOffsetsToTxn(offsets map[string][]*sarama.PartitionOffsetMetadata, groupID string) error {
	return nil
}
func (f *fakeSyncProducer) AddMessageToTxn(msg *sarama.ConsumerMessage, groupID string, metadata *string) error {
	return nil
}
func (f *fakeSyncProducer) AddOffsetsToTxnWithGroupMetadata(offsets map[string][]*sarama.PartitionOffsetMetadata, groupMetadata *sarama.ConsumerGroupMetadata) error {
	return nil
}
func (f *fakeSyncProducer) AddMessageToTxnWithGroupMetadata(msg *sarama.ConsumerMessage, groupMetadata *sarama.ConsumerGroupMetadata, metadata *string) error {
	return nil
}

var _ SyncProducer = (*fakeSyncProducer)(nil)

type fakeGroupConsumer struct{}

func (f *fakeGroupConsumer) Start(ctx context.Context) error { return ctx.Err() }
func (f *fakeGroupConsumer) Close() error                    { return nil }

var _ GroupConsumer = (*fakeGroupConsumer)(nil)

func TestMockImplementations(t *testing.T) {
	var _ Producer = (*syncProducer)(nil)
	var _ SyncProducer = (*fakeSyncProducer)(nil)
	var _ GroupConsumer = (*fakeGroupConsumer)(nil)
}
