package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/weaviate/weaviate-go-client/v4/weaviate/data/replication"
	"github.com/weaviate/weaviate/entities/models"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()

	log.Println("Patching objects...")
	b, err := os.ReadFile("data.json")
	if err != nil {
		log.Fatalf("failed to read file: %v", err)
	}

	var objects []*models.Object
	err = json.Unmarshal(b, &objects)
	if err != nil {
		log.Fatalf("failed to unmarshal objects: %v", err)
	}

	for i, obj := range objects {
		err := node1Client.Data().Updater().
			WithMerge().
			WithClassName(class.Class).
			WithID(obj.ID.String()).
			WithProperties(map[string]interface{}{
				"name": fmt.Sprintf("patched!obj#%d", i),
			}).
			WithConsistencyLevel(replication.ConsistencyLevel.QUORUM).
			Do(ctx)
		if err != nil {
			log.Fatalf("failed to patch object %s", obj.ID)
		}
	}
}
