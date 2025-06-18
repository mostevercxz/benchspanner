package main

import (
	"benchspanner/metrics"
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	databasepb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
	"google.golang.org/grpc/status"
)

const (
	projectID  = "superb-receiver-463215-f7"
	databaseID = "mtbench"
	graphName  = "graph0618"
)

// 基准测试配置
var (
	VUS                   = 10                             // 并发用户数
	ZONE_START            = 100                            // 起始区ID
	ZONES_TOTAL           = 10                             // 总区数
	RECORDS_PER_ZONE      = 80                             // 每区玩家数
	EDGES_PER_RELATION    = 100                            // 每种关系的边数
	TOTAL_VERTICES        = ZONES_TOTAL * RECORDS_PER_ZONE // 总顶点数（使用固定的 IDS_PER_ZONE = 8000）
	STR_ATTR_CNT          = 10                             // 字符串属性数量
	INT_ATTR_CNT          = 90                             // 整数属性数量
	PreGenerateVertexData = true                           // 是否预生成所有顶点数据
	instanceID            = "graph-demo"
)

// VertexData represents a vertex to be inserted
type VertexData struct {
	UID      int64
	StrAttrs []string
	IntAttrs []int64
}

// setupGraphSpanner creates all tables, indexes, TTL policies, and the property graph.
func setupGraphSpanner(ctx context.Context) error {
	admin, err := database.NewDatabaseAdminClient(ctx, option.WithCredentialsFile(`superb-receiver-463215-f7-3b974ed0b146.json`))
	if err != nil {
		if st, ok := status.FromError(err); ok {
			log.Printf("NewDatabaseAdminClient failed - Code: %v (%d), Message: %s",
				st.Code(), int(st.Code()), st.Message())
			for _, detail := range st.Details() {
				log.Printf("Error Detail: %+v", detail)
			}
		} else {
			log.Printf("NewDatabaseAdminClient failed - Error: %v", err)
		}
		return err
	}
	defer admin.Close()

	dbPath := fmt.Sprintf(
		"projects/%s/instances/%s/databases/%s",
		projectID, instanceID, databaseID,
	)

	var ddl []string

	// 1. Clean slate
	ddl = append(ddl, fmt.Sprintf("DROP PROPERTY GRAPH IF EXISTS %s", graphName))
	edgeLabels := []string{"Rel1", "Rel2", "Rel3", "Rel4", "Rel5"}
	for _, label := range edgeLabels {
		ddl = append(ddl, fmt.Sprintf("DROP TABLE IF EXISTS %s", label))
	}
	ddl = append(ddl, "DROP INDEX IF EXISTS user_attr11_attr12_attr13_idx")
	ddl = append(ddl, "DROP TABLE IF EXISTS Users")

	// 2. Vertex table
	ddl = append(ddl, `
CREATE TABLE Users (
  uid          INT64  NOT NULL,
  attr1        STRING(20),
  attr2        STRING(20),
  attr3        STRING(20),
  attr4        STRING(20),
  attr5        STRING(20),
  attr6        STRING(20),
  attr7        STRING(20),
  attr8        STRING(20),
  attr9        STRING(20),
  attr10       STRING(20),
  attr11       INT64,
  attr12       INT64,
  attr13       INT64,
  attr14       INT64,
  attr15       INT64,
  attr16       INT64,
  attr17       INT64,
  attr18       INT64,
  attr19       INT64,
  attr20       INT64,
  attr21       INT64,
  attr22       INT64,
  attr23       INT64,
  attr24       INT64,
  attr25       INT64,
  attr26       INT64,
  attr27       INT64,
  attr28       INT64,
  attr29       INT64,
  attr30       INT64,
  attr31       INT64,
  attr32       INT64,
  attr33       INT64,
  attr34       INT64,
  attr35       INT64,
  attr36       INT64,
  attr37       INT64,
  attr38       INT64,
  attr39       INT64,
  attr40       INT64,
  attr41       INT64,
  attr42       INT64,
  attr43       INT64,
  attr44       INT64,
  attr45       INT64,
  attr46       INT64,
  attr47       INT64,
  attr48       INT64,
  attr49       INT64,
  attr50       INT64,
  attr51       INT64,
  attr52       INT64,
  attr53       INT64,
  attr54       INT64,
  attr55       INT64,
  attr56       INT64,
  attr57       INT64,
  attr58       INT64,
  attr59       INT64,
  attr60       INT64,
  attr61       INT64,
  attr62       INT64,
  attr63       INT64,
  attr64       INT64,
  attr65       INT64,
  attr66       INT64,
  attr67       INT64,
  attr68       INT64,
  attr69       INT64,
  attr70       INT64,
  attr71       INT64,
  attr72       INT64,
  attr73       INT64,
  attr74       INT64,
  attr75       INT64,
  attr76       INT64,
  attr77       INT64,
  attr78       INT64,
  attr79       INT64,
  attr80       INT64,
  attr81       INT64,
  attr82       INT64,
  attr83       INT64,
  attr84       INT64,
  attr85       INT64,
  attr86       INT64,
  attr87       INT64,
  attr88       INT64,
  attr89       INT64,
  attr90       INT64,
  attr91       INT64,
  attr92       INT64,
  attr93       INT64,
  attr94       INT64,
  attr95       INT64,
  attr96       INT64,
  attr97       INT64,
  attr98       INT64,
  attr99       INT64,
  attr100      INT64,
  expire_time  TIMESTAMP OPTIONS (allow_commit_timestamp=true)
) PRIMARY KEY (uid)`)

	ddl = append(ddl,
		`CREATE INDEX user_attr11_attr12_attr13_idx
		   ON Users(attr11, attr12, attr13)`,
		`ALTER TABLE Users
		   ADD ROW DELETION POLICY (OLDER_THAN(expire_time, INTERVAL 8 DAY))`,
	)

	// 3. Edge tables + indexes + TTL (MODIFIED)
	for _, label := range edgeLabels {
		// FIXED: Renamed src_uid to uid to match parent table's PK for interleaving
		ddl = append(ddl, fmt.Sprintf(`
CREATE TABLE %s (
  uid          INT64      NOT NULL,
  dst_uid      INT64      NOT NULL,
  attr101      INT64,
  attr102      INT64,
  attr103      INT64,
  attr104      INT64,
  attr105      INT64,
  attr106      INT64,
  attr107      INT64,
  attr108      INT64,
  attr109      INT64,
  attr110      INT64,
  expire_time  TIMESTAMP OPTIONS (allow_commit_timestamp=true)
) PRIMARY KEY (uid, dst_uid),
  INTERLEAVE IN PARENT Users ON DELETE CASCADE`, label))

		// FIXED: Index now references the renamed 'uid' column
		ddl = append(ddl, fmt.Sprintf(
			`CREATE INDEX %s_uid_attr_covering_idx
			   ON %s(uid, attr101, attr102, attr103)`,
			strings.ToLower(label), label))

		ddl = append(ddl, fmt.Sprintf(
			`ALTER TABLE %s
			   ADD ROW DELETION POLICY (OLDER_THAN(expire_time, INTERVAL 8 DAY))`,
			label))
	}

	// 4. Property graph definition (MODIFIED)
	var edgeDefs []string
	for _, l := range edgeLabels {
		// FIXED: SOURCE KEY now correctly references 'uid'
		edgeDefs = append(edgeDefs, fmt.Sprintf(`
  %s
    SOURCE KEY (uid) REFERENCES Users(uid)
    DESTINATION KEY (dst_uid) REFERENCES Users(uid)
    LABEL %s PROPERTIES (attr101, attr102, attr103, attr104, attr105, attr106, attr107, attr108, attr109, attr110)`, l, l))
	}

	userProps := []string{"uid"}
	for i := 1; i <= 100; i++ {
		userProps = append(userProps, fmt.Sprintf("attr%d", i))
	}

	graphDDL := fmt.Sprintf(`CREATE PROPERTY GRAPH %s
NODE TABLES (
  Users KEY (uid)
    LABEL User PROPERTIES (%s)
)
EDGE TABLES (%s
)`, graphName, strings.Join(userProps, ", "), strings.Join(edgeDefs, ","))

	ddl = append(ddl, graphDDL)

	// print all the ddl to the terminal
	log.Println(strings.Join(ddl, "\n\n"))

	time.Sleep(100 * time.Second)

	// 5. Push DDL to Spanner
	op, err := admin.UpdateDatabaseDdl(ctx, &databasepb.UpdateDatabaseDdlRequest{
		Database:   dbPath,
		Statements: ddl,
	})
	if err != nil {
		if st, ok := status.FromError(err); ok {
			log.Printf("UpdateDatabaseDdl failed - Code: %v (%d), Message: %s",
				st.Code(), int(st.Code()), st.Message())
			for _, detail := range st.Details() {
				log.Printf("Error Detail: %+v", detail)
			}
		} else {
			log.Printf("UpdateDatabaseDdl failed - Error: %v", err)
		}
		return err
	}
	if err := op.Wait(ctx); err != nil {
		if st, ok := status.FromError(err); ok {
			log.Printf("DDL operation wait failed - Code: %v (%d), Message: %s",
				st.Code(), int(st.Code()), st.Message())
			for _, detail := range st.Details() {
				log.Printf("Error Detail: %+v", detail)
			}
		} else {
			log.Printf("DDL operation wait failed - Error: %v", err)
		}
		return err
	}

	log.Printf("Graph %q created successfully in %s", graphName, dbPath)
	return nil
}

func spannerTruncateAllData(ctx context.Context, dbPath string) error {
	// 1. Create a Spanner data client (not an admin client)
	client, err := spanner.NewClient(ctx, dbPath,
		option.WithCredentialsFile(`superb-receiver-463215-f7-3b974ed0b146.json`))
	if err != nil {
		log.Printf("Failed to create Spanner client: %v", err)
		return err
	}
	defer client.Close()

	// 2. Define the tables to be cleared, in the correct dependency order.
	// Child tables must be cleared before the parent table.
	edgeTables := []string{"Rel1", "Rel2", "Rel3", "Rel4", "Rel5"}
	parentTable := "Users"

	// 3. Delete data from all child (edge) tables first.
	log.Println("Starting to delete data from edge tables...")
	for _, table := range edgeTables {
		// Use PartitionedUpdate for bulk deletes. The `WHERE true` clause selects all rows.
		stmt := spanner.Statement{SQL: fmt.Sprintf("DELETE FROM %s WHERE true", table)}

		count, err := client.PartitionedUpdate(ctx, stmt)
		if err != nil {
			log.Printf("Partitioned DML failed for table %s: %v", table, err)
			return err
		}
		log.Printf("Deleted %d rows from table %s\n", count, table)
	}

	// 4. Once all child tables are empty, delete data from the parent table.
	log.Println("Starting to delete data from Users table...")
	stmt := spanner.Statement{SQL: fmt.Sprintf("DELETE FROM %s WHERE true", parentTable)}

	count, err := client.PartitionedUpdate(ctx, stmt)
	if err != nil {
		log.Printf("Partitioned DML failed for table %s: %v", parentTable, err)
		return err
	}
	log.Printf("Deleted %d rows from table %s\n", count, parentTable)

	log.Println("All graph data has been successfully deleted.")
	return nil
}

// generateVertexData generates vertex data in memory
func generateVertexData(zoneStart, zonesTotal, recordsPerZone, strAttrCnt, intAttrCnt int) ([]*VertexData, error) {
	totalRecords := zonesTotal * recordsPerZone
	data := make([]*VertexData, 0, totalRecords)

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	letters := []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

	for zoneID := zoneStart; zoneID < zoneStart+zonesTotal; zoneID++ {
		for id := 1; id <= recordsPerZone; id++ {
			uid := (int64(zoneID) << 40) | int64(id)

			// Generate string attributes
			strAttrs := make([]string, strAttrCnt)
			for i := 0; i < strAttrCnt; i++ {
				strAttrs[i] = randFixedString(rng, letters, 20)
			}

			// Generate integer attributes
			intAttrs := make([]int64, intAttrCnt)
			for i := 0; i < intAttrCnt; i++ {
				intAttrs[i] = rng.Int63n(10000)
			}

			vertex := &VertexData{
				UID:      uid,
				StrAttrs: strAttrs,
				IntAttrs: intAttrs,
			}

			data = append(data, vertex)
		}
	}

	log.Printf("Generated %d vertex records", len(data))
	return data, nil
}

func randFixedString(rng *rand.Rand, pool []rune, n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = pool[rng.Intn(len(pool))]
	}
	return string(b)
}

func countdownOrExit(action string, seconds int) {
	log.Printf("即将%s，%d秒后启动。按 Ctrl+C 可中断...", action, seconds)

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)

	timer := time.NewTimer(time.Duration(seconds) * time.Second)
	select {
	case <-interrupt:
		log.Println("用户中断，程序退出。")
		os.Exit(1)
	case <-timer.C:
		// Continue normally
	}
}

// spannerWriteVertexTest performs vertex write testing with multiple goroutines
func spannerWriteVertexTest(client *spanner.Client, preGenerate bool) {
	log.Println("Starting Spanner vertex write test...")

	var wg sync.WaitGroup
	startTime := time.Now()

	// Initialize metrics
	metricsCollector := metrics.NewConcurrentMetrics(VUS)

	totalVertices := ZONES_TOTAL * RECORDS_PER_ZONE
	verticesPerWorker := int(math.Ceil(float64(totalVertices) / float64(VUS)))

	var vertices []*VertexData

	if preGenerate {
		// Step 1: Generate vertex data in memory (original behavior)
		log.Println("Generating vertex data in memory...")
		var err error
		vertices, err = generateVertexData(ZONE_START, ZONES_TOTAL, RECORDS_PER_ZONE, STR_ATTR_CNT, INT_ATTR_CNT)
		if err != nil {
			log.Printf("Failed to generate vertex data: %s", err.Error())
			return
		}
		log.Printf("Distributing %d vertices among %d workers...", len(vertices), VUS)
	} else {
		log.Printf("Workers will generate vertices on-the-fly. Total: %d vertices, %d workers...", totalVertices, VUS)
	}

	log.Printf("Starting %d write workers...", VUS)
	maxDelay := 100 * time.Microsecond
	// co := spanner.CommitOptions{MaxCommitDelay: &maxDelay}
	// applyOpts := []spanner.ApplyOption{
	// 	// spanner.ApplyAtLeastOnce(),     // 1-RPC fast path
	// 	spanner.ApplyCommitOptions(co), // commit delay
	// }
	countdownOrExit("开始写入顶点", 5)

	for worker := 0; worker < VUS; worker++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			// Calculate data range for this worker
			startIdx := workerID * verticesPerWorker
			endIdx := (workerID + 1) * verticesPerWorker
			if endIdx > totalVertices {
				endIdx = totalVertices
			}

			log.Printf("Worker %d started, processing vertices %d to %d", workerID, startIdx, endIdx-1)

			workerSuccessCount := 0
			workerErrorCount := 0

			// Initialize random generator for this worker (used when preGenerate=false)
			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID)))
			letters := []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

			// Process vertices assigned to this worker
			for i := startIdx; i < endIdx; i++ {
				var vertex *VertexData

				if preGenerate {
					// Use pre-generated data
					vertex = vertices[i]
				} else {
					// Generate vertex on-the-fly
					zoneOffset := i / RECORDS_PER_ZONE
					idInZone := i%RECORDS_PER_ZONE + 1
					zoneID := ZONE_START + zoneOffset
					uid := (int64(zoneID) << 40) | int64(idInZone)

					// Generate string attributes
					strAttrs := make([]string, STR_ATTR_CNT)
					for j := 0; j < STR_ATTR_CNT; j++ {
						strAttrs[j] = randFixedString(rng, letters, 20)
					}

					// Generate integer attributes
					intAttrs := make([]int64, INT_ATTR_CNT)
					for j := 0; j < INT_ATTR_CNT; j++ {
						intAttrs[j] = rng.Int63n(10000)
					}

					vertex = &VertexData{
						UID:      uid,
						StrAttrs: strAttrs,
						IntAttrs: intAttrs,
					}
				}

				// Build mutation for this vertex
				mutation := buildVertexMutation(vertex)

				// Execute insert
				insertStart := time.Now()
				_, err := client.Apply(context.Background(), []*spanner.Mutation{mutation})
				insertDuration := time.Since(insertStart)

				// Record metrics
				metricsCollector.AddDuration(workerID, insertDuration)

				if err != nil {
					log.Printf("Worker %d insert failed for UID %d: %s", workerID, vertex.UID, err.Error())
					workerErrorCount++
					metricsCollector.AddError(1)
				} else {
					workerSuccessCount++
					metricsCollector.AddSuccess(1)
				}

				// Log progress every 100 inserts
				if (i-startIdx+1)%100 == 0 {
					log.Printf("Worker %d processed %d vertices, success: %d, errors: %d",
						workerID, i-startIdx+1, workerSuccessCount, workerErrorCount)
				}
			}

			log.Printf("Worker %d completed: %d success, %d errors",
				workerID, workerSuccessCount, workerErrorCount)
		}(worker)
	}

	// Wait for all workers to complete
	wg.Wait()
	totalDuration := time.Since(startTime)

	// Get combined metrics
	combinedMetrics := metricsCollector.CombinedStats()
	totalSuccess := metricsCollector.GetSuccessCount()
	totalErrors := metricsCollector.GetErrorCount()

	log.Println("Vertex write test completed:")
	log.Printf("  Total duration: %v", totalDuration)
	log.Printf("  Total vertices: %d", totalVertices)
	log.Printf("  Successful inserts: %d", totalSuccess)
	log.Printf("  Failed inserts: %d", totalErrors)
	log.Printf("  Success rate: %.2f%%", float64(totalSuccess)*100/float64(totalVertices))
	log.Printf("  Throughput: %.2f vertices/sec", float64(totalSuccess)/totalDuration.Seconds())
	log.Printf("  Latency metrics: %s", combinedMetrics.String())
}

// buildVertexMutation builds a Spanner mutation for inserting a vertex
func buildVertexMutation(vertex *VertexData) *spanner.Mutation {
	// Prepare columns and values for the Users table
	columns := []string{"uid"}
	values := []interface{}{vertex.UID}

	// Add string attributes (attr1-attr10)
	for i, strAttr := range vertex.StrAttrs {
		columns = append(columns, fmt.Sprintf("attr%d", i+1))
		values = append(values, strAttr)
	}

	// Add integer attributes (attr11-attr100)
	for i, intAttr := range vertex.IntAttrs {
		attrIndex := len(vertex.StrAttrs) + i + 1
		columns = append(columns, fmt.Sprintf("attr%d", attrIndex))
		values = append(values, intAttr)
	}

	// Add expire_time for TTL
	columns = append(columns, "expire_time")
	values = append(values, spanner.CommitTimestamp)

	return spanner.Insert("Users", columns, values)
}

// spannerWriteEdgeTest performs edge write testing with multiple goroutines
func spannerWriteEdgeTest(client *spanner.Client, startZoneID, endZoneID int, batchNum int) {
	log.Printf("Starting Spanner edge write test for zones [%d, %d) with batch size %d...", startZoneID, endZoneID, batchNum)

	// Calculate total zones and players
	totalZones := endZoneID - startZoneID
	totalPlayers := int64(totalZones * RECORDS_PER_ZONE)
	totalEdges := totalPlayers * 5 * int64(EDGES_PER_RELATION) // 5种关系，每种边数由EDGES_PER_RELATION配置

	log.Printf("Total zones: %d, players per zone: %d", totalZones, RECORDS_PER_ZONE)
	log.Printf("Total players: %d, Total edges to insert: %d", totalPlayers, totalEdges)

	// Assign players to workers
	playersPerWorker := int(math.Ceil(float64(totalPlayers) / float64(VUS)))

	var wg sync.WaitGroup
	startTime := time.Now()

	// Initialize metrics
	metricsCollector := metrics.NewConcurrentMetrics(VUS)

	log.Printf("Starting %d edge write workers...", VUS)
	countdownOrExit("开始写入边", 5)

	for worker := 0; worker < VUS; worker++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			log.Printf("Worker %d started", workerID)

			workerSuccessCount := 0
			workerErrorCount := 0

			// Calculate players assigned to this worker
			workerStartIndex := workerID * playersPerWorker
			workerEndIndex := (workerID + 1) * playersPerWorker
			if workerEndIndex > int(totalPlayers) {
				workerEndIndex = int(totalPlayers)
			}

			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID)))

			// Process players assigned to this worker
			for playerIndex := workerStartIndex; playerIndex < workerEndIndex; playerIndex++ {
				// Convert playerIndex to corresponding UID
				zoneOffset := playerIndex / RECORDS_PER_ZONE
				idInZone := playerIndex%RECORDS_PER_ZONE + 1
				currentZoneID := startZoneID + zoneOffset
				playerUID := (int64(currentZoneID) << 40) | int64(idInZone)

				// Create 5 types of relationships for this player
				relationTypes := []string{"Rel1", "Rel2", "Rel3", "Rel4", "Rel5"}

				for _, relType := range relationTypes {
					// Create mutations for EDGES_PER_RELATION edges of this relationship type
					var mutations []*spanner.Mutation

					for i := 0; i < EDGES_PER_RELATION; i++ {
						// Randomly select target UID
						targetZoneID := ZONE_START + rng.Intn(ZONES_TOTAL)
						targetID := 1 + rng.Intn(RECORDS_PER_ZONE)
						targetUID := (int64(targetZoneID) << 40) | int64(targetID)

						if targetUID == playerUID {
							// Skip self-loop
							log.Printf("ERROR: Worker %d skipped self-loop for player %d (%s),targetZoneid=%d,targetID=%d", workerID, playerUID, relType, targetZoneID, targetID)
							continue
						}

						// Generate edge attributes attr101-attr110
						edgeAttrs := make([]int64, 10)
						for j := 0; j < 10; j++ {
							edgeAttrs[j] = rng.Int63n(10000)
						}

						// Build mutation for this edge
						mutation := buildEdgeMutation(relType, playerUID, targetUID, edgeAttrs)
						mutations = append(mutations, mutation)

						// Execute batch when reaching batchNum or at the end
						if len(mutations) >= batchNum || i == EDGES_PER_RELATION-1 {
							insertStart := time.Now()
							_, err := client.Apply(context.Background(), mutations)
							insertDuration := time.Since(insertStart)

							// Record metrics for the batch
							metricsCollector.AddDuration(workerID, insertDuration)

							if err != nil {
								log.Printf("Worker %d edge batch insert failed for player %d (%s), batch size %d: %s",
									workerID, playerUID, relType, len(mutations), err.Error())
								workerErrorCount += len(mutations)
								metricsCollector.AddError(int64(len(mutations)))
							} else {
								workerSuccessCount += len(mutations)
								metricsCollector.AddSuccess(int64(len(mutations)))
							}

							// Reset mutations slice for next batch
							mutations = []*spanner.Mutation{}
						}
					}
				}

				// Log progress every 10 players
				if (playerIndex-workerStartIndex+1)%10 == 0 {
					log.Printf("Worker %d processed player %d (UID: %d), success: %d, errors: %d",
						workerID, playerIndex, playerUID, workerSuccessCount, workerErrorCount)
				}
			}

			log.Printf("Worker %d completed: %d success, %d errors",
				workerID, workerSuccessCount, workerErrorCount)
		}(worker)
	}

	// Wait for all workers to complete
	wg.Wait()
	totalDuration := time.Since(startTime)

	// Get combined metrics
	combinedMetrics := metricsCollector.CombinedStats()
	totalSuccess := metricsCollector.GetSuccessCount()
	totalErrors := metricsCollector.GetErrorCount()

	log.Println("Edge write test completed:")
	log.Printf("  Total duration: %v", totalDuration)
	log.Printf("  Total edges expected: %d", totalEdges)
	log.Printf("  Successful inserts: %d", totalSuccess)
	log.Printf("  Failed inserts: %d", totalErrors)
	log.Printf("  Success rate: %.2f%%", float64(totalSuccess)*100/float64(totalEdges))
	log.Printf("  Throughput: %.2f edges/sec", float64(totalSuccess)/totalDuration.Seconds())
	log.Printf("  Latency metrics: %s", combinedMetrics.String())
}

// buildEdgeMutation builds a Spanner mutation for inserting an edge
func buildEdgeMutation(relType string, sourceUID, targetUID int64, attrs []int64) *spanner.Mutation {
	// Prepare columns and values for the edge table
	columns := []string{"uid", "dst_uid"}
	values := []interface{}{sourceUID, targetUID}

	// Add edge attributes attr101-attr110
	for i, attr := range attrs {
		columns = append(columns, fmt.Sprintf("attr%d", 101+i))
		values = append(values, attr)
	}

	// Add expire_time for TTL
	columns = append(columns, "expire_time")
	values = append(values, spanner.CommitTimestamp)

	return spanner.Insert(relType, columns, values)
}

// spannerReadRelationTest performs edge relation read testing with multiple goroutines – similar logic to polardbReadRelationTest.
// It scans every vertex within the configured zone range, reads up to 300 destination UIDs connected via
// Rel1 / Rel4 / Rel5 edges that satisfy attr101>1000, attr102>2000, attr103>4000 and records latency metrics.
func spannerReadRelationTest(ctx context.Context, dbPath string) {
	log.Println("Starting Spanner relation read test…")

	totalVertices := ZONES_TOTAL * RECORDS_PER_ZONE
	iterPerVU := int(math.Ceil(float64(totalVertices) / float64(VUS)))

	var wg sync.WaitGroup
	startTime := time.Now()

	// Metrics
	metricsCollector := metrics.NewConcurrentMetrics(VUS)

	log.Printf("Starting %d read workers…", VUS)
	countdownOrExit("开始读取关系", 5)

	for vu := 0; vu < VUS; vu++ {
		wg.Add(1)
		go func(vuIndex int) {
			defer wg.Done()
			log.Printf("VU %d started, will process %d vertices", vuIndex, iterPerVU)
			client, err := spanner.NewClient(ctx, dbPath, option.WithCredentialsFile(`superb-receiver-463215-f7-3b974ed0b146.json`))
			if err != nil {
				log.Printf("Failed to create Spanner client: %v", err)
				return
			}
			defer client.Close()

			ctx := context.Background()

			const baseSQL = `
			GRAPH %s
			MATCH (a:User {uid:@uid})-[e:Rel1|Rel4|Rel5]->(b:User)
			WHERE e.attr101 > @a101 AND e.attr102 > @a102 AND e.attr103 > @a103
			RETURN b.uid, b.attr1, b.attr2, b.attr3, b.attr4, b.attr5, b.attr6, b.attr7, b.attr8, b.attr9, b.attr10,
			       b.attr11, b.attr12, b.attr13, b.attr14, b.attr15, b.attr16, b.attr17, b.attr18, b.attr19, b.attr20,
			       b.attr21, b.attr22, b.attr23, b.attr24, b.attr25, b.attr26, b.attr27, b.attr28, b.attr29, b.attr30,
			       b.attr31, b.attr32, b.attr33, b.attr34, b.attr35, b.attr36, b.attr37, b.attr38, b.attr39, b.attr40,
			       b.attr41, b.attr42, b.attr43, b.attr44, b.attr45, b.attr46, b.attr47, b.attr48, b.attr49, b.attr50,
			       b.attr51, b.attr52, b.attr53, b.attr54, b.attr55, b.attr56, b.attr57, b.attr58, b.attr59, b.attr60,
			       b.attr61, b.attr62, b.attr63, b.attr64, b.attr65, b.attr66, b.attr67, b.attr68, b.attr69, b.attr70,
			       b.attr71, b.attr72, b.attr73, b.attr74, b.attr75, b.attr76, b.attr77, b.attr78, b.attr79, b.attr80,
			       b.attr81, b.attr82, b.attr83, b.attr84, b.attr85, b.attr86, b.attr87, b.attr88, b.attr89, b.attr90,
			       b.attr91, b.attr92, b.attr93, b.attr94, b.attr95, b.attr96, b.attr97, b.attr98, b.attr99, b.attr100
			LIMIT 300`

			stmtTpl := spanner.Statement{
				SQL: fmt.Sprintf(baseSQL, graphName), // stable text!
				Params: map[string]interface{}{ // literals become params
					"a101": int64(1000),
					"a102": int64(2000),
					"a103": int64(4000),
				},
			}

			for iter := 0; iter < iterPerVU; iter++ {
				idx := vuIndex*iterPerVU + iter
				if idx >= totalVertices {
					break
				}

				// Derive UID from idx
				zoneOffset := idx / RECORDS_PER_ZONE
				idInZone := idx%RECORDS_PER_ZONE + 1
				zoneID := ZONE_START + zoneOffset
				uid := (int64(zoneID) << 40) | int64(idInZone)

				stmt := stmtTpl          // shallow copy
				stmt.Params["uid"] = uid // just mutate the param

				queryStart := time.Now()
				// Create a new single-use read-only transaction for each query
				ro := client.Single()
				iterRows := ro.Query(ctx, stmt)

				// Consume rows and capture errors
				rowCnt := 0
				success := true
				for {
					row, err := iterRows.Next()
					if err == iterator.Done {
						break
					}
					if err != nil {
						log.Printf("VU %d query failed for uid %d: %v", vuIndex, uid, err)
						success = false
						break
					}
					printSpannerRow(row)
					rowCnt++
				}
				iterRows.Stop()
				ro.Close() // Close the transaction after each query

				// Measure the complete query duration including result consumption
				queryDuration := time.Since(queryStart)
				metricsCollector.AddDuration(vuIndex, queryDuration)

				if success {
					metricsCollector.AddSuccess(1)
				} else {
					metricsCollector.AddError(1)
				}

				// Log progress every 100 queries
				if iter%10 == 0 {
					log.Printf("VU %d, iter %d, uid %d, query time: %v, rows: %d", vuIndex, iter, uid, queryDuration, rowCnt)
				}
			}

			log.Printf("VU %d completed", vuIndex)
		}(vu)
	}

	wg.Wait()
	totalDuration := time.Since(startTime)

	combinedMetrics := metricsCollector.CombinedStats()
	totalSuccess := metricsCollector.GetSuccessCount()
	totalErrors := metricsCollector.GetErrorCount()

	log.Println("Relation read test completed:")
	log.Printf("  Total duration: %v", totalDuration)
	log.Printf("  Total queries: %d", totalSuccess+totalErrors)
	log.Printf("  Successful queries: %d", totalSuccess)
	log.Printf("  Failed queries: %d", totalErrors)
	log.Printf("  Success rate: %.2f%%", float64(totalSuccess)*100/float64(totalSuccess+totalErrors))
	log.Printf("  Throughput: %.2f queries/sec", float64(totalSuccess)/totalDuration.Seconds())
	log.Printf("  Latency metrics: %s", combinedMetrics.String())
}

// initFromEnv initializes configuration from environment variables
func initFromEnv() {
	// Initialize VUS from environment variable
	if vusStr := os.Getenv("VUS"); vusStr != "" {
		if parsedVUS, err := strconv.Atoi(vusStr); err == nil && parsedVUS > 0 {
			VUS = parsedVUS
		}
	}

	// Initialize ZONE_START from environment variable
	if zoneStartStr := os.Getenv("ZONE_START"); zoneStartStr != "" {
		if parsedZoneStart, err := strconv.Atoi(zoneStartStr); err == nil && parsedZoneStart >= 0 {
			ZONE_START = parsedZoneStart
		}
	}

	// Initialize ZONES_TOTAL from environment variable
	if zonesTotalStr := os.Getenv("ZONES_TOTAL"); zonesTotalStr != "" {
		if parsedZonesTotal, err := strconv.Atoi(zonesTotalStr); err == nil && parsedZonesTotal > 0 {
			ZONES_TOTAL = parsedZonesTotal
		}
	}

	// Initialize RECORDS_PER_ZONE from environment variable
	if recordsPerZoneStr := os.Getenv("RECORDS_PER_ZONE"); recordsPerZoneStr != "" {
		if parsedRecordsPerZone, err := strconv.Atoi(recordsPerZoneStr); err == nil && parsedRecordsPerZone > 0 {
			RECORDS_PER_ZONE = parsedRecordsPerZone
		}
	}

	// Initialize EDGES_PER_RELATION from environment variable
	if edgesPerRelationStr := os.Getenv("EDGES_PER_RELATION"); edgesPerRelationStr != "" {
		if parsedEdgesPerRelation, err := strconv.Atoi(edgesPerRelationStr); err == nil && parsedEdgesPerRelation > 0 {
			EDGES_PER_RELATION = parsedEdgesPerRelation
		}
	}

	// Initialize PreGenerateVertexData from environment variable
	if preGenStr := os.Getenv("PRE_GENERATE_VERTEX_DATA"); preGenStr != "" {
		lower := strings.ToLower(preGenStr)
		if lower == "false" || lower == "0" {
			PreGenerateVertexData = false
		} else if lower == "true" || lower == "1" {
			PreGenerateVertexData = true
		}
	}

	// Recalculate TOTAL_VERTICES after configuration changes
	TOTAL_VERTICES = ZONES_TOTAL * RECORDS_PER_ZONE

	// Initialize instanceID from environment variable
	if instID := os.Getenv("INSTANCE_ID"); instID != "" {
		instanceID = instID
	}

	log.Printf("Configuration: VUS=%d, ZONE_START=%d, ZONES_TOTAL=%d, RECORDS_PER_ZONE=%d, EDGES_PER_RELATION=%d, TOTAL_VERTICES=%d, PreGenerateVertexData=%v",
		VUS, ZONE_START, ZONES_TOTAL, RECORDS_PER_ZONE, EDGES_PER_RELATION, TOTAL_VERTICES, PreGenerateVertexData)
}

// setupLogging configures logging to output to both terminal and file
func setupLogging() *os.File {
	// Create logs directory if it doesn't exist
	logsDir := "logs"
	if err := os.MkdirAll(logsDir, 0755); err != nil {
		log.Printf("Failed to create logs directory: %v", err)
		return nil
	}

	// Create log file with timestamp
	timestamp := time.Now().Format("2006-01-02_15")
	logFileName := fmt.Sprintf("benchmark_%s.log", timestamp)
	logFilePath := filepath.Join(logsDir, logFileName)

	// Open log file
	logFile, err := os.OpenFile(logFilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Printf("Failed to open log file %s: %v", logFilePath, err)
		return nil
	}

	// Create MultiWriter to write to both stdout and file
	multiWriter := io.MultiWriter(os.Stdout, logFile)

	// Set log output to MultiWriter
	log.SetOutput(multiWriter)
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	log.Printf("Logging initialized - output to terminal and file: %s", logFilePath)
	return logFile
}

// Add helper printer near other helpers
func printSpannerRow(row *spanner.Row) {
	if row == nil {
		log.Println("<nil row>")
		return
	}

	cols := row.Size()
	var sb strings.Builder
	sb.WriteString("Row: {")
	for i := 0; i < cols; i++ {
		if i > 0 {
			sb.WriteString(", ")
		}
		colName := row.ColumnName(i)

		// Try to decode as different types based on column name or try common types
		var value interface{}
		var err error

		// For uid columns, try INT64 first
		if strings.Contains(colName, "uid") {
			var intVal int64
			err = row.Column(i, &intVal)
			if err == nil {
				value = intVal
			}
		} else if strings.HasPrefix(colName, "attr1") && len(colName) <= 6 && colName != "attr11" && colName != "attr12" && colName != "attr13" && colName != "attr14" && colName != "attr15" && colName != "attr16" && colName != "attr17" && colName != "attr18" && colName != "attr19" {
			// attr1-attr10 are STRING columns
			var strVal spanner.NullString
			err = row.Column(i, &strVal)
			if err == nil {
				if strVal.Valid {
					value = strVal.StringVal
				} else {
					value = "<NULL>"
				}
			}
		} else {
			// attr11-attr100 are INT64 columns
			var intVal spanner.NullInt64
			err = row.Column(i, &intVal)
			if err == nil {
				if intVal.Valid {
					value = intVal.Int64
				} else {
					value = "<NULL>"
				}
			}
		}

		if err != nil {
			sb.WriteString(fmt.Sprintf("%s: <err %v>", colName, err))
		} else {
			sb.WriteString(fmt.Sprintf("%s: %v", colName, value))
		}
	}
	sb.WriteString("}")
	log.Println(sb.String())
}

func main() {
	// Setup logging to output to both terminal and file
	logFile := setupLogging()
	if logFile != nil {
		defer logFile.Close()
	}

	// Initialize configuration from environment variables
	initFromEnv()

	// Define command line flags
	var testType string
	var startZone, endZone int
	var batchNum int
	flag.StringVar(&testType, "test", "setup", "Test type to run: setup, write-vertex, write-edge, relation, all")
	flag.IntVar(&startZone, "start-zone", ZONE_START, "Start zone ID for edge test")
	flag.IntVar(&endZone, "end-zone", ZONE_START+ZONES_TOTAL, "End zone ID for edge test")
	flag.IntVar(&batchNum, "batch-num", EDGES_PER_RELATION, "Number of edges per batch for edge write test")
	flag.Parse()

	ctx := context.Background()

	// Fully-qualified database name:
	//   projects/{PROJECT}/instances/{INSTANCE}/databases/{DATABASE}
	dbPath := fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instanceID, databaseID)

	// If you set GOOGLE_APPLICATION_CREDENTIALS, you can omit the option.
	client, err := spanner.NewClient(ctx, dbPath,
		option.WithCredentialsFile(`superb-receiver-463215-f7-3b974ed0b146.json`))
	if err != nil {
		if st, ok := status.FromError(err); ok {
			log.Printf("spanner.NewClient failed - Code: %v (%d), Message: %s",
				st.Code(), int(st.Code()), st.Message())
			for _, detail := range st.Details() {
				log.Printf("Error Detail: %+v", detail)
			}
		} else {
			log.Printf("spanner.NewClient failed - Error: %v", err)
		}
		log.Fatalf("Failed to create Spanner client")
	}
	defer client.Close()

	log.Printf("Starting Spanner benchmark with test type: %s", testType)

	// Execute tests based on command line option
	switch testType {
	case "setup":
		log.Println("Running setup test...")

		// Simple read to prove it works
		stmt := spanner.Statement{SQL: "SELECT 1 AS one"}
		iter := client.Single().Query(ctx, stmt)
		defer iter.Stop()

		var one int64
		row, err := iter.Next()
		if err != nil {
			if st, ok := status.FromError(err); ok {
				log.Printf("Query failed - Code: %v (%d), Message: %s",
					st.Code(), int(st.Code()), st.Message())
				for _, detail := range st.Details() {
					log.Printf("Error Detail: %+v", detail)
				}
			} else {
				log.Printf("Query failed - Error: %v", err)
			}
			log.Fatalf("Query execution failed")
		}
		if err := row.Column(0, &one); err != nil {
			if st, ok := status.FromError(err); ok {
				log.Printf("Failed to parse row - Code: %v (%d), Message: %s",
					st.Code(), int(st.Code()), st.Message())
				for _, detail := range st.Details() {
					log.Printf("Error Detail: %+v", detail)
				}
			} else {
				log.Printf("Failed to parse row - Error: %v", err)
			}
			log.Fatalf("Row parsing failed")
		}
		fmt.Printf("Got %d from Spanner!\n", one)

		// Setup graph schema
		if err := setupGraphSpanner(ctx); err != nil {
			log.Fatalf("Failed to setup graph: %v", err)
		}

	case "write-vertex":
		log.Println("Running vertex write test...")
		spannerWriteVertexTest(client, PreGenerateVertexData)

	case "write-edge":
		log.Printf("Running edge write test for zones [%d, %d)...", startZone, endZone)
		spannerWriteEdgeTest(client, startZone, endZone, batchNum)

	case "truncate":
		log.Println("Running truncate test...")
		spannerTruncateAllData(ctx, dbPath)

	case "relation":
		log.Println("Running relation read test...")
		spannerReadRelationTest(ctx, dbPath)

	case "all":
		log.Println("Running all tests...")

		// First setup the graph if needed
		if err := setupGraphSpanner(ctx); err != nil {
			log.Printf("Setup failed: %v", err)
		}

		// Test vertex write performance
		spannerWriteVertexTest(client, PreGenerateVertexData)

		// Test edge write performance
		spannerWriteEdgeTest(client, ZONE_START, ZONE_START+ZONES_TOTAL, batchNum)

		// Test relation read performance
		spannerReadRelationTest(ctx, dbPath)

	default:
		log.Printf("Unknown test type: %s", testType)
		log.Println("Available test types: setup, write-vertex, write-edge, relation, all")
		os.Exit(1)
	}

	log.Println("Benchmark completed")
}
