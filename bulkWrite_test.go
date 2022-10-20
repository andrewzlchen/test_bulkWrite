package writes_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func createUpdate(numElements int) []mongo.WriteModel {
	writes := make([]mongo.WriteModel, 0, numElements)

	for i := 0; i < numElements; i++ {
		writes = append(writes, mongo.NewUpdateOneModel().
			SetFilter(bson.D{{"name", fmt.Sprintf("name%d", time.Now().Unix())}}).
			SetUpdate(bson.D{{"$set", bson.D{{"foo", "bar"}}}}),
		)
	}
	return writes
}

func BenchmarkBulkWrite(b *testing.B) {

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI("mongodb://localhost:27017"))
	if err != nil {
		b.Errorf("failed to create client: %s", err)
		b.Fail()
	}

	coll := client.Database("foo").Collection("bar")

	b.Run("when writing 1000 updates", func(b *testing.B) {

		b.Run("bulkWrite in batches of 1", func(b *testing.B) {
			for i := 0; i < 1000; i++ {
				b.StopTimer()
				updates := createUpdate(1)
				b.StartTimer()
				_, err := coll.BulkWrite(context.Background(), updates)
				if err != nil {
					fmt.Printf("failed to bulkWrite: %s\n", err)
					b.Fail()
				}
				b.StopTimer()
			}
		})

		b.Run("bulkWrite in batches of 10", func(b *testing.B) {
			for i := 0; i < 100; i++ {
				b.StopTimer()
				updates := createUpdate(10)
				b.StartTimer()
				_, err := coll.BulkWrite(context.Background(), updates)
				if err != nil {
					fmt.Printf("failed to bulkWrite: %s\n", err)
					b.Fail()
				}
				b.StopTimer()
			}
		})

		b.Run("bulkWrites in batches of 100", func(b *testing.B) {
			for i := 0; i < 10; i++ {
				b.StopTimer()
				updates := createUpdate(100)
				b.StartTimer()
				_, err := coll.BulkWrite(context.Background(), updates)
				if err != nil {
					fmt.Printf("failed to bulkWrite: %s\n", err)
					b.Fail()
				}
				b.StopTimer()
			}
		})

		b.Run("bulkWrites in batches of 1000", func(b *testing.B) {
			for i := 0; i < 1; i++ {
				b.StopTimer()
				updates := createUpdate(1000)
				b.StartTimer()
				_, err := coll.BulkWrite(context.Background(), updates)
				if err != nil {
					fmt.Printf("failed to bulkWrite: %s\n", err)
					b.Fail()
				}
				b.StopTimer()
			}
		})

	})
}
