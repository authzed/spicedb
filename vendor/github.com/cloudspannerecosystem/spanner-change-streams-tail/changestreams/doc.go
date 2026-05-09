//
// Copyright 2022 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

/*
Package changestreams provides the functionality for reading the Cloud Spanner change streams.

# Example

	package main

	import (
		"context"
		"fmt"
		"log"

		"github.com/cloudspannerecosystem/spanner-change-streams-tail/changestreams"
	)

	func main() {
		ctx := context.Background()
		reader, err := changestreams.NewReader(ctx, "myproject", "myinstance", "mydb", "mystream")
		if err != nil {
			log.Fatalf("failed to create a reader: %v", err)
		}
		defer reader.Close()

		if err := reader.Read(ctx, func(result *changestreams.ReadResult) error {
			for _, cr := range result.ChangeRecords {
				for _, dcr := range cr.DataChangeRecords {
					fmt.Printf("[%s] %s %s\n", dcr.CommitTimestamp, dcr.ModType, dcr.TableName)
				}
			}
			return nil
		}); err != nil {
			log.Fatalf("failed to read: %v", err)
		}
	}
*/
package changestreams
