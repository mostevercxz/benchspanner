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
	"sync/atomic"
	"time"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	databasepb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
	"google.golang.org/grpc/status"
)

const (
	graphName = "graph0618"
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
	BATCH_NUM             = 1                              // 批量写入大小
	EDGES_TO_FETCH        = 1000                           // 更新边测试中要获取的边数
	UPDATES_PER_EDGE      = 10                             // 每条边要更新的次数
	instanceID            = "graph-demo"
	GRAPH_NAME            = "graph0618"
	credentialsFile       = "superb-receiver-463215-f7-3b974ed0b146.json" // GCP credentials file path
	projectID             = "superb-receiver-463215-f7"
	databaseID            = "mtbench"
)

// VertexData represents a vertex to be inserted
type VertexData struct {
	UID      int64
	StrAttrs []string
	IntAttrs []int64
}

type EdgeData struct {
	SourceUID int64
	TargetUID int64
	RelType   string
	Shard     int64
}

// setupTableWithoutIndex creates all tables, TTL policies, and the property graph but excludes indexes.
func setupTableWithoutIndex(ctx context.Context) error {
	admin, err := database.NewDatabaseAdminClient(ctx, option.WithCredentialsFile(credentialsFile))
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
	ddl = append(ddl, fmt.Sprintf("DROP PROPERTY GRAPH IF EXISTS %s", GRAPH_NAME))
	edgeLabels := []string{"Rel1", "Rel2", "Rel3", "Rel4", "Rel5"}
	for _, label := range edgeLabels {
		ddl = append(ddl, fmt.Sprintf("DROP TABLE IF EXISTS %s", label))
	}
	ddl = append(ddl, "DROP INDEX IF EXISTS user_attr11_attr12_attr13_idx")
	for _, label := range edgeLabels {
		ddl = append(ddl, fmt.Sprintf("DROP INDEX IF EXISTS %s_uid_attr_covering_idx", strings.ToLower(label)))
	}
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

	// Add TTL policy for Users table
	ddl = append(ddl,
		`ALTER TABLE Users
		   ADD ROW DELETION POLICY (OLDER_THAN(expire_time, INTERVAL 8 DAY))`,
	)

	// 3. Edge tables + TTL (without indexes)
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

		ddl = append(ddl, fmt.Sprintf(
			`ALTER TABLE %s
			   ADD ROW DELETION POLICY (OLDER_THAN(expire_time, INTERVAL 8 DAY))`,
			label))
	}

	// 4. Property graph definition
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
)`, GRAPH_NAME, strings.Join(userProps, ", "), strings.Join(edgeDefs, ","))

	ddl = append(ddl, graphDDL)

	// print all the ddl to the terminal
	log.Println("Tables and Property Graph DDL:")
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

	log.Printf("Tables and graph %q created successfully in %s", GRAPH_NAME, dbPath)
	return nil
}

// setupAllTableIndexes creates all indexes for the tables.
func setupAllTableIndexes(ctx context.Context) error {
	admin, err := database.NewDatabaseAdminClient(ctx, option.WithCredentialsFile(credentialsFile))
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
	edgeLabels := []string{"Rel1", "Rel2", "Rel3", "Rel4", "Rel5"}

	// Create index for Users table
	ddl = append(ddl,
		`CREATE INDEX user_attr11_attr12_attr13_idx
		   ON Users(attr11, attr12, attr13)`,
	)

	// Create indexes for edge tables
	for _, label := range edgeLabels {
		ddl = append(ddl, fmt.Sprintf(
			`CREATE INDEX %s_uid_attr_covering_idx
			   ON %s(uid, attr101, attr102, attr103)`,
			strings.ToLower(label), label))
	}

	// print all the ddl to the terminal
	log.Println("Index DDL:")
	log.Println(strings.Join(ddl, "\n\n"))

	time.Sleep(100 * time.Second)

	// Push DDL to Spanner
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

	log.Printf("All indexes created successfully in %s", dbPath)
	return nil
}

// setupGraphSpanner creates all tables, indexes, TTL policies, and the property graph.
func setupGraphSpanner(ctx context.Context) error {
	// First create tables without indexes
	if err := setupTableWithoutIndex(ctx); err != nil {
		return err
	}

	// Then create all indexes
	if err := setupAllTableIndexes(ctx); err != nil {
		return err
	}

	return nil
}

func spannerTruncateAllData(ctx context.Context, dbPath string) error {
	// 1. Create a Spanner data client (not an admin client)
	client, err := spanner.NewClient(ctx, dbPath,
		option.WithCredentialsFile(credentialsFile))
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
	// maxDelay := 100 * time.Microsecond
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
				if (playerIndex-workerStartIndex+1)%100 == 0 {
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

func fetchExistingEdges(client *spanner.Client, limit int) ([]*EdgeData, error) {
	log.Printf("Fetching %d existing edges from Spanner...", limit)

	ctx := context.Background()
	var edges []*EdgeData

	// Query edges from different relation types
	relationTypes := []string{"Rel1", "Rel2", "Rel3", "Rel4", "Rel5"}
	edgesPerType := limit / len(relationTypes)

	for _, relType := range relationTypes {
		stmt := spanner.Statement{
			SQL: fmt.Sprintf("SELECT shard, uid, dst_uid FROM %s LIMIT %d", relType, edgesPerType),
		}

		iter := client.Single().Query(ctx, stmt)
		defer iter.Stop()

		for {
			row, err := iter.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				return nil, fmt.Errorf("failed to query %s: %v", relType, err)
			}

			var shard, uid, dstUID int64
			if err := row.Columns(&shard, &uid, &dstUID); err != nil {
				return nil, fmt.Errorf("failed to parse row: %v", err)
			}

			edges = append(edges, &EdgeData{
				SourceUID: uid,
				TargetUID: dstUID,
				RelType:   relType,
				Shard:     shard,
			})
		}
	}

	log.Printf("Successfully fetched %d existing edges", len(edges))
	return edges, nil
}

// buildEdgeUpdateMutation builds a Spanner mutation for updating edge attributes
func buildEdgeUpdateMutation(relType string, sourceUID, targetUID int64, attr101, attr102, attr103 int64) *spanner.Mutation {
	// Calculate shard for this edge
	shard := calcShard(sourceUID)

	// Update specific attributes - need to include key columns for the update
	columns := []string{"shard", "uid", "dst_uid", "attr101", "attr102", "attr103"}
	values := []interface{}{shard, sourceUID, targetUID, attr101, attr102, attr103}

	return spanner.Update(relType, columns, values)
}

// spannerUpdateEdgeTest performs edge update testing with multiple goroutines
func spannerUpdateEdgeTest(client *spanner.Client) {
	log.Printf("Starting Spanner edge update test...")
	log.Printf("Configuration: Fetch %d existing edges, update each edge %d times", EDGES_TO_FETCH, UPDATES_PER_EDGE)

	// Step 1: Fetch existing edges from the database
	log.Printf("Step 1: Fetching existing edges...")
	existingEdges, err := fetchExistingEdges(client, EDGES_TO_FETCH)
	if err != nil {
		log.Printf("Failed to fetch existing edges: %v", err)
		return
	}

	if len(existingEdges) == 0 {
		log.Printf("No existing edges found in the database. Please run edge write test first.")
		return
	}

	log.Printf("Successfully fetched %d existing edges", len(existingEdges))

	// Step 2: Calculate total updates and distribution
	totalUpdates := int64(len(existingEdges) * UPDATES_PER_EDGE)
	edgesPerWorker := int(math.Ceil(float64(len(existingEdges)) / float64(VUS)))

	log.Printf("Total updates to perform: %d (each of %d edges updated %d times)", totalUpdates, len(existingEdges), UPDATES_PER_EDGE)
	log.Printf("Edges per worker: %d", edgesPerWorker)

	var wg sync.WaitGroup
	startTime := time.Now()

	// Initialize metrics
	metricsCollector := metrics.NewConcurrentMetrics(VUS)

	// Setup signal handling for graceful shutdown
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)
	var stopFlag int32

	log.Printf("Starting %d edge update workers...", VUS)

	// 倒计时处理
	countdownOrExit("开始更新边", 5)

	// Start signal handler goroutine
	go func() {
		<-interrupt
		log.Printf("Received termination signal, stopping update edge test gracefully...")
		atomic.StoreInt32(&stopFlag, 1)
	}()

	for worker := 0; worker < VUS; worker++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			log.Printf("Worker %d started", workerID)

			workerSuccessCount := 0
			workerErrorCount := 0

			// Create random generator for this worker
			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID)))

			// Calculate which edges this worker should process
			workerStartEdge := workerID * edgesPerWorker
			workerEndEdge := (workerID + 1) * edgesPerWorker
			if workerEndEdge > len(existingEdges) {
				workerEndEdge = len(existingEdges)
			}

			// Process edges assigned to this worker
			for edgeIndex := workerStartEdge; edgeIndex < workerEndEdge; edgeIndex++ {
				// Check stop flag for outer loop
				if atomic.LoadInt32(&stopFlag) == 1 {
					log.Printf("Worker %d received stop signal, terminating at edgeIndex %d", workerID, edgeIndex)
					break
				}

				edge := existingEdges[edgeIndex]

				// Update this edge UPDATES_PER_EDGE times
				for updateRound := 0; updateRound < UPDATES_PER_EDGE; updateRound++ {
					// Check stop flag for inner loop
					if atomic.LoadInt32(&stopFlag) == 1 {
						log.Printf("Worker %d received stop signal, terminating at edgeIndex %d, updateRound %d", workerID, edgeIndex, updateRound)
						break
					}

					// Generate random attribute values for update (attr101, attr102, attr103)
					attr101 := rng.Int63n(10000)
					attr102 := rng.Int63n(10000)
					attr103 := rng.Int63n(10000)

					// Build and execute UPDATE mutation
					updateStart := time.Now()
					mutation := buildEdgeUpdateMutation(edge.RelType, edge.SourceUID, edge.TargetUID, attr101, attr102, attr103)

					// Debug: log the first few mutations
					if workerID == 0 && edgeIndex == workerStartEdge && updateRound == 0 {
						log.Printf("Sample edge update: %s edge %d->%d, attrs: %d,%d,%d",
							edge.RelType, edge.SourceUID, edge.TargetUID, attr101, attr102, attr103)
					}

					_, err := client.Apply(context.Background(), []*spanner.Mutation{mutation})
					updateDuration := time.Since(updateStart)

					// Record metrics
					metricsCollector.AddDuration(workerID, updateDuration)

					if err != nil {
						log.Printf("Worker %d update failed for edge %d->%d [%s]: %s",
							workerID, edge.SourceUID, edge.TargetUID, edge.RelType, err.Error())
						workerErrorCount++
						metricsCollector.AddError(1)
					} else {
						workerSuccessCount++
						metricsCollector.AddSuccess(1)
					}
				}

				// Log progress every 100 edges
				if (edgeIndex-workerStartEdge+1)%100 == 0 {
					log.Printf("Worker %d processed %d edges (success: %d, errors: %d)",
						workerID, edgeIndex-workerStartEdge+1, workerSuccessCount, workerErrorCount)
				}
			}

			log.Printf("Worker %d completed: processed %d edges with %d updates each (success: %d, errors: %d)",
				workerID, workerEndEdge-workerStartEdge, UPDATES_PER_EDGE, workerSuccessCount, workerErrorCount)
		}(worker)
	}

	// Wait for all workers to complete
	wg.Wait()
	totalDuration := time.Since(startTime)

	// Get combined metrics
	combinedMetrics := metricsCollector.CombinedStats()
	totalSuccess := metricsCollector.GetSuccessCount()
	totalErrors := metricsCollector.GetErrorCount()

	log.Println("Edge update test completed:")
	log.Printf("  Total duration: %v", totalDuration)
	log.Printf("  Edges fetched: %d", len(existingEdges))
	log.Printf("  Updates per edge: %d", UPDATES_PER_EDGE)
	log.Printf("  Total updates attempted: %d", totalUpdates)
	log.Printf("  Successful updates: %d", totalSuccess)
	log.Printf("  Failed updates: %d", totalErrors)
	log.Printf("  Success rate: %.2f%%", float64(totalSuccess)*100/float64(totalUpdates))
	log.Printf("  Throughput: %.2f updates/sec", float64(totalSuccess)/totalDuration.Seconds())
	log.Printf("  Latency metrics: %s", combinedMetrics.String())
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

	log.Printf("Starting %d read workers…,dbpath=%s", VUS, dbPath)
	countdownOrExit("开始读取关系", 5)

	for vu := 0; vu < VUS; vu++ {
		wg.Add(1)
		go func(vuIndex int) {
			defer wg.Done()
			log.Printf("VU %d started, will process %d vertices", vuIndex, iterPerVU)
			client, err := spanner.NewClient(ctx, dbPath, option.WithCredentialsFile(credentialsFile))
			if err != nil {
				log.Printf("Failed to create Spanner client: %v", err)
				return
			}
			defer client.Close()

			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(vuIndex)))

			ctx := context.Background()

			const baseSQL = `
			GRAPH %s
			MATCH (a:User {uid:@uid})-[e:Rel1|Rel4|Rel5]->(b:User)
			WHERE e.attr101 > @a101 AND e.attr102 > @a102 AND e.attr103 > @a103
			RETURN a.uid AS a_uid, a.attr1 AS a_attr1, a.attr2 AS a_attr2, a.attr3 AS a_attr3, a.attr4 AS a_attr4, a.attr5 AS a_attr5, a.attr6 AS a_attr6, a.attr7 AS a_attr7, a.attr8 AS a_attr8, a.attr9 AS a_attr9, a.attr10 AS a_attr10,
			       a.attr11 AS a_attr11, a.attr12 AS a_attr12, a.attr13 AS a_attr13, a.attr14 AS a_attr14, a.attr15 AS a_attr15, a.attr16 AS a_attr16, a.attr17 AS a_attr17, a.attr18 AS a_attr18, a.attr19 AS a_attr19, a.attr20 AS a_attr20,
			       a.attr21 AS a_attr21, a.attr22 AS a_attr22, a.attr23 AS a_attr23, a.attr24 AS a_attr24, a.attr25 AS a_attr25, a.attr26 AS a_attr26, a.attr27 AS a_attr27, a.attr28 AS a_attr28, a.attr29 AS a_attr29, a.attr30 AS a_attr30,
			       a.attr31 AS a_attr31, a.attr32 AS a_attr32, a.attr33 AS a_attr33, a.attr34 AS a_attr34, a.attr35 AS a_attr35, a.attr36 AS a_attr36, a.attr37 AS a_attr37, a.attr38 AS a_attr38, a.attr39 AS a_attr39, a.attr40 AS a_attr40,
			       a.attr41 AS a_attr41, a.attr42 AS a_attr42, a.attr43 AS a_attr43, a.attr44 AS a_attr44, a.attr45 AS a_attr45, a.attr46 AS a_attr46, a.attr47 AS a_attr47, a.attr48 AS a_attr48, a.attr49 AS a_attr49, a.attr50 AS a_attr50,
			       a.attr51 AS a_attr51, a.attr52 AS a_attr52, a.attr53 AS a_attr53, a.attr54 AS a_attr54, a.attr55 AS a_attr55, a.attr56 AS a_attr56, a.attr57 AS a_attr57, a.attr58 AS a_attr58, a.attr59 AS a_attr59, a.attr60 AS a_attr60,
			       a.attr61 AS a_attr61, a.attr62 AS a_attr62, a.attr63 AS a_attr63, a.attr64 AS a_attr64, a.attr65 AS a_attr65, a.attr66 AS a_attr66, a.attr67 AS a_attr67, a.attr68 AS a_attr68, a.attr69 AS a_attr69, a.attr70 AS a_attr70,
			       a.attr71 AS a_attr71, a.attr72 AS a_attr72, a.attr73 AS a_attr73, a.attr74 AS a_attr74, a.attr75 AS a_attr75, a.attr76 AS a_attr76, a.attr77 AS a_attr77, a.attr78 AS a_attr78, a.attr79 AS a_attr79, a.attr80 AS a_attr80,
			       a.attr81 AS a_attr81, a.attr82 AS a_attr82, a.attr83 AS a_attr83, a.attr84 AS a_attr84, a.attr85 AS a_attr85, a.attr86 AS a_attr86, a.attr87 AS a_attr87, a.attr88 AS a_attr88, a.attr89 AS a_attr89, a.attr90 AS a_attr90,
			       a.attr91 AS a_attr91, a.attr92 AS a_attr92, a.attr93 AS a_attr93, a.attr94 AS a_attr94, a.attr95 AS a_attr95, a.attr96 AS a_attr96, a.attr97 AS a_attr97, a.attr98 AS a_attr98, a.attr99 AS a_attr99, a.attr100 AS a_attr100,
			       b.uid AS b_uid, b.attr1 AS b_attr1, b.attr2 AS b_attr2, b.attr3 AS b_attr3, b.attr4 AS b_attr4, b.attr5 AS b_attr5, b.attr6 AS b_attr6, b.attr7 AS b_attr7, b.attr8 AS b_attr8, b.attr9 AS b_attr9, b.attr10 AS b_attr10,
			       b.attr11 AS b_attr11, b.attr12 AS b_attr12, b.attr13 AS b_attr13, b.attr14 AS b_attr14, b.attr15 AS b_attr15, b.attr16 AS b_attr16, b.attr17 AS b_attr17, b.attr18 AS b_attr18, b.attr19 AS b_attr19, b.attr20 AS b_attr20,
			       b.attr21 AS b_attr21, b.attr22 AS b_attr22, b.attr23 AS b_attr23, b.attr24 AS b_attr24, b.attr25 AS b_attr25, b.attr26 AS b_attr26, b.attr27 AS b_attr27, b.attr28 AS b_attr28, b.attr29 AS b_attr29, b.attr30 AS b_attr30,
			       b.attr31 AS b_attr31, b.attr32 AS b_attr32, b.attr33 AS b_attr33, b.attr34 AS b_attr34, b.attr35 AS b_attr35, b.attr36 AS b_attr36, b.attr37 AS b_attr37, b.attr38 AS b_attr38, b.attr39 AS b_attr39, b.attr40 AS b_attr40,
			       b.attr41 AS b_attr41, b.attr42 AS b_attr42, b.attr43 AS b_attr43, b.attr44 AS b_attr44, b.attr45 AS b_attr45, b.attr46 AS b_attr46, b.attr47 AS b_attr47, b.attr48 AS b_attr48, b.attr49 AS b_attr49, b.attr50 AS b_attr50,
			       b.attr51 AS b_attr51, b.attr52 AS b_attr52, b.attr53 AS b_attr53, b.attr54 AS b_attr54, b.attr55 AS b_attr55, b.attr56 AS b_attr56, b.attr57 AS b_attr57, b.attr58 AS b_attr58, b.attr59 AS b_attr59, b.attr60 AS b_attr60,
			       b.attr61 AS b_attr61, b.attr62 AS b_attr62, b.attr63 AS b_attr63, b.attr64 AS b_attr64, b.attr65 AS b_attr65, b.attr66 AS b_attr66, b.attr67 AS b_attr67, b.attr68 AS b_attr68, b.attr69 AS b_attr69, b.attr70 AS b_attr70,
			       b.attr71 AS b_attr71, b.attr72 AS b_attr72, b.attr73 AS b_attr73, b.attr74 AS b_attr74, b.attr75 AS b_attr75, b.attr76 AS b_attr76, b.attr77 AS b_attr77, b.attr78 AS b_attr78, b.attr79 AS b_attr79, b.attr80 AS b_attr80,
			       b.attr81 AS b_attr81, b.attr82 AS b_attr82, b.attr83 AS b_attr83, b.attr84 AS b_attr84, b.attr85 AS b_attr85, b.attr86 AS b_attr86, b.attr87 AS b_attr87, b.attr88 AS b_attr88, b.attr89 AS b_attr89, b.attr90 AS b_attr90,
			       b.attr91 AS b_attr91, b.attr92 AS b_attr92, b.attr93 AS b_attr93, b.attr94 AS b_attr94, b.attr95 AS b_attr95, b.attr96 AS b_attr96, b.attr97 AS b_attr97, b.attr98 AS b_attr98, b.attr99 AS b_attr99, b.attr100 AS b_attr100
			LIMIT 300`

			stmtTpl := spanner.Statement{
				SQL: fmt.Sprintf(baseSQL, GRAPH_NAME), // stable text!
				Params: map[string]interface{}{ // literals become params
					"a101": rng.Intn(10000),
					"a102": rng.Intn(10000),
					"a103": rng.Intn(10000),
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
					_, err := iterRows.Next()
					if err == iterator.Done {
						break
					}
					if err != nil {
						log.Printf("VU %d query failed for uid %d: %v", vuIndex, uid, err)
						success = false
						break
					}
					// printSpannerRow(row)
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
				if iter%500 == 0 {
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

// spannerWriteBatchVertexTest performs vertex write testing with multiple goroutines using batch inserts
func spannerWriteBatchVertexTest(client *spanner.Client, batchNum int) {
	log.Printf("Starting Spanner batch vertex write test with batch size %d...", batchNum)

	var wg sync.WaitGroup
	startTime := time.Now()

	// Initialize metrics
	metricsCollector := metrics.NewConcurrentMetrics(VUS)

	totalVertices := ZONES_TOTAL * RECORDS_PER_ZONE
	verticesPerWorker := int(math.Ceil(float64(totalVertices) / float64(VUS)))

	log.Printf("Workers will generate vertices on-the-fly. Total: %d vertices, %d workers, batch size: %d...", totalVertices, VUS, batchNum)

	log.Printf("Starting %d write workers...", VUS)
	countdownOrExit("开始批量写入顶点", 5)

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

			// Initialize random generator for this worker
			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID)))
			letters := []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

			// Batch mutations
			var mutations []*spanner.Mutation

			// Process vertices assigned to this worker
			for i := startIdx; i < endIdx; i++ {
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

				vertex := &VertexData{
					UID:      uid,
					StrAttrs: strAttrs,
					IntAttrs: intAttrs,
				}

				// Build mutation for this vertex
				mutation := buildVertexMutation(vertex)
				mutations = append(mutations, mutation)

				// Execute batch when reaching batchNum or at the end
				if len(mutations) >= batchNum || i == endIdx-1 {
					insertStart := time.Now()
					_, err := client.Apply(context.Background(), mutations)
					insertDuration := time.Since(insertStart)

					// Record metrics for the batch
					metricsCollector.AddDuration(workerID, insertDuration)

					if err != nil {
						log.Printf("Worker %d batch insert failed, batch size %d: %s", workerID, len(mutations), err.Error())
						workerErrorCount += len(mutations)
						metricsCollector.AddError(int64(len(mutations)))
					} else {
						workerSuccessCount += len(mutations)
						metricsCollector.AddSuccess(int64(len(mutations)))
					}

					// Reset mutations slice for next batch
					mutations = []*spanner.Mutation{}
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

	log.Println("Batch vertex write test completed:")
	log.Printf("  Total duration: %v", totalDuration)
	log.Printf("  Total vertices: %d", totalVertices)
	log.Printf("  Batch size: %d", batchNum)
	log.Printf("  Successful inserts: %d", totalSuccess)
	log.Printf("  Failed inserts: %d", totalErrors)
	log.Printf("  Success rate: %.2f%%", float64(totalSuccess)*100/float64(totalVertices))
	log.Printf("  Throughput: %.2f vertices/sec", float64(totalSuccess)/totalDuration.Seconds())
	log.Printf("  Latency metrics: %s", combinedMetrics.String())
}

// spannerUpdateVertexTest performs vertex update testing with multiple goroutines
func spannerUpdateVertexTest(client *spanner.Client) {
	log.Println("Starting Spanner vertex update test...")

	totalVertices := int64(ZONES_TOTAL * RECORDS_PER_ZONE)
	totalUpdates := totalVertices // Each worker will perform updates on existing vertices

	// Calculate updates per worker
	updatesPerWorker := int(math.Ceil(float64(totalUpdates) / float64(VUS)))

	var wg sync.WaitGroup
	startTime := time.Now()

	// Initialize metrics
	metricsCollector := metrics.NewConcurrentMetrics(VUS)

	log.Printf("Starting %d update workers, total updates: %d...", VUS, totalUpdates)
	countdownOrExit("开始更新顶点属性", 5)

	for worker := 0; worker < VUS; worker++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			log.Printf("Worker %d started", workerID)

			workerSuccessCount := 0
			workerErrorCount := 0

			// Create random generator for this worker
			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID)))

			// Calculate which updates this worker should process
			startUpdate := workerID * updatesPerWorker
			endUpdate := (workerID + 1) * updatesPerWorker
			if endUpdate > int(totalUpdates) {
				endUpdate = int(totalUpdates)
			}

			for updateIndex := startUpdate; updateIndex < endUpdate; updateIndex++ {
				// Calculate UID from update index (similar to vertex generation)
				zoneOffset := updateIndex / RECORDS_PER_ZONE
				idInZone := updateIndex%RECORDS_PER_ZONE + 1
				zoneID := ZONE_START + zoneOffset
				uid := (int64(zoneID) << 40) | int64(idInZone)

				// Generate random attribute values for update
				// We'll update attr88, attr89, attr90, attr91, attr92
				attr88 := rng.Int63n(10000)
				attr89 := rng.Int63n(10000)
				attr90 := rng.Int63n(10000)
				attr91 := rng.Int63n(10000)
				attr92 := rng.Int63n(10000)

				// Build mutation for this vertex update
				mutation := buildVertexUpdateMutation(uid, attr88, attr89, attr90, attr91, attr92)

				// Execute update
				updateStart := time.Now()
				_, err := client.Apply(context.Background(), []*spanner.Mutation{mutation})
				updateDuration := time.Since(updateStart)

				// Record metrics
				metricsCollector.AddDuration(workerID, updateDuration)

				if err != nil {
					log.Printf("Worker %d update failed for UID %d: %s", workerID, uid, err.Error())
					workerErrorCount++
					metricsCollector.AddError(1)
				} else {
					workerSuccessCount++
					metricsCollector.AddSuccess(1)
				}

				// Log progress every 100 updates
				if (updateIndex-startUpdate+1)%100 == 0 {
					log.Printf("Worker %d processed %d updates (success: %d, errors: %d)",
						workerID, updateIndex-startUpdate+1, workerSuccessCount, workerErrorCount)
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

	log.Println("Vertex update test completed:")
	log.Printf("  Total duration: %v", totalDuration)
	log.Printf("  Total updates: %d", totalUpdates)
	log.Printf("  Successful updates: %d", totalSuccess)
	log.Printf("  Failed updates: %d", totalErrors)
	log.Printf("  Success rate: %.2f%%", float64(totalSuccess)*100/float64(totalUpdates))
	log.Printf("  Throughput: %.2f updates/sec", float64(totalSuccess)/totalDuration.Seconds())
	log.Printf("  Latency metrics: %s", combinedMetrics.String())
}

// buildVertexUpdateMutation builds a Spanner mutation for updating vertex attributes
func buildVertexUpdateMutation(uid, attr88, attr89, attr90, attr91, attr92 int64) *spanner.Mutation {

	// Update specific attributes - need to include key columns for the update
	columns := []string{"uid", "attr88", "attr89", "attr90", "attr91", "attr92"}
	values := []interface{}{uid, attr88, attr89, attr90, attr91, attr92}

	return spanner.Update("Users", columns, values)
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

	// Initialize GRAPH_NAME from environment variable with default value "g0618"
	if graphNameStr := os.Getenv("GRAPH_NAME"); graphNameStr != "" {
		GRAPH_NAME = graphNameStr
	} else {
		GRAPH_NAME = "g0618"
	}
	log.Printf("GRAPH_NAME set to: %s", GRAPH_NAME)

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

	// Initialize BATCH_NUM from environment variable
	if batchNumStr := os.Getenv("BATCH_NUM"); batchNumStr != "" {
		if parsedBatchNum, err := strconv.Atoi(batchNumStr); err == nil && parsedBatchNum > 0 {
			BATCH_NUM = parsedBatchNum
		}
	}

	// Initialize credentialsFile from environment variable
	if credFile := os.Getenv("GOOGLE_APPLICATION_CREDENTIALS_FILE"); credFile != "" {
		credentialsFile = credFile
	}

	// Initialize projectID from environment variable
	if projectIDStr := os.Getenv("PROJECT_ID"); projectIDStr != "" {
		projectID = projectIDStr
	}

	// Initialize databaseID from environment variable
	if dbID := os.Getenv("DATABASE_ID"); dbID != "" {
		databaseID = dbID
	}

	if edgesToFetchStr := os.Getenv("EDGES_TO_FETCH"); edgesToFetchStr != "" {
		if parsedEdgesToFetch, err := strconv.Atoi(edgesToFetchStr); err == nil && parsedEdgesToFetch > 0 {
			EDGES_TO_FETCH = parsedEdgesToFetch
		}
	}

	// Initialize UPDATES_PER_EDGE from environment variableAdd commentMore actions
	if updatesPerEdgeStr := os.Getenv("UPDATES_PER_EDGE"); updatesPerEdgeStr != "" {
		if parsedUpdatesPerEdge, err := strconv.Atoi(updatesPerEdgeStr); err == nil && parsedUpdatesPerEdge > 0 {
			UPDATES_PER_EDGE = parsedUpdatesPerEdge
		}
	}

	log.Printf("Configuration: VUS=%d, ZONE_START=%d, ZONES_TOTAL=%d, RECORDS_PER_ZONE=%d, EDGES_PER_RELATION=%d, TOTAL_VERTICES=%d, PreGenerateVertexData=%v, BATCH_NUM=%d, EDGES_TO_FETCH=%d, UPDATES_PER_EDGE=%d, credentialsFile=%s",
		VUS, ZONE_START, ZONES_TOTAL, RECORDS_PER_ZONE, EDGES_PER_RELATION, TOTAL_VERTICES, PreGenerateVertexData, BATCH_NUM, EDGES_TO_FETCH, UPDATES_PER_EDGE, credentialsFile)
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

// spannerReadInvertTest performs invert read testing with multiple goroutinesAdd commentMore actions
func spannerReadInvertTest(client *spanner.Client) {
	log.Println("Starting Spanner invert read test...")

	// Total queries to execute
	totalQueries := 5000000
	iterPerVU := int(math.Ceil(float64(totalQueries) / float64(VUS)))

	var wg sync.WaitGroup
	startTime := time.Now()

	// Initialize metrics
	metricsCollector := metrics.NewConcurrentMetrics(VUS)

	// Setup signal handling for graceful shutdown
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)
	var stopFlag int32

	log.Printf("Starting %d invert read workers...", VUS)

	// 倒计时处理
	countdownOrExit("开始倒排读取", 5)

	// Start signal handler goroutine
	go func() {
		<-interrupt
		log.Printf("Received termination signal, stopping read-invert test gracefully...")
		atomic.StoreInt32(&stopFlag, 1)
	}()

	for vu := 0; vu < VUS; vu++ {
		wg.Add(1)
		go func(vuIndex int) {
			defer wg.Done()
			log.Printf("VU %d started, need to process %d queries", vuIndex, iterPerVU)

			// Create random generator for this worker
			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(vuIndex)))

			ctx := context.Background()

			// Execute iterations assigned to this VU
			for iter := 0; iter < iterPerVU; iter++ {
				// Check stop flag
				if atomic.LoadInt32(&stopFlag) == 1 {
					log.Printf("VU %d received stop signal, terminating at iter %d", vuIndex, iter)
					break
				}

				totalProcessed := vuIndex*iterPerVU + iter
				if totalProcessed >= totalQueries {
					break
				}

				// Generate random values for WHERE clause [0,9999)
				attr11Value := rng.Intn(9999)
				attr12Value := rng.Intn(9999)
				attr13Value := rng.Intn(9999)

				// Build parameterized query
				stmt := spanner.Statement{
					SQL: `SELECT * FROM Users 
						  WHERE attr11 = @attr11 AND attr12 > @attr12 AND attr13 > @attr13 
						  LIMIT 300`,
					Params: map[string]interface{}{
						"attr11": int64(attr11Value),
						"attr12": int64(attr12Value),
						"attr13": int64(attr13Value),
					},
				}

				queryStart := time.Now()
				// Create a new single-use read-only transaction for each query
				ro := client.Single()
				iterRows := ro.Query(ctx, stmt)

				// Count rows and consume results
				rowCount := 0
				success := true
				for {
					_, err := iterRows.Next()
					if err == iterator.Done {
						break
					}
					if err != nil {
						log.Printf("VU %d query failed: %v", vuIndex, err)
						success = false
						break
					}
					rowCount++
				}
				iterRows.Stop()
				ro.Close() // Close the transaction after each query

				queryDuration := time.Since(queryStart)

				// Record metrics
				metricsCollector.AddDuration(vuIndex, queryDuration)

				if success {
					metricsCollector.AddSuccess(1)
				} else {
					metricsCollector.AddError(1)
				}

				// Log progress every 100 queries
				if iter%100 == 0 {
					log.Printf("VU %d, iter %d, attr11=%d, attr12>%d, attr13>%d, query time: %v, rows: %d",
						vuIndex, iter, attr11Value, attr12Value, attr13Value, queryDuration, rowCount)
				}
			}

			log.Printf("VU %d completed", vuIndex)
		}(vu)
	}

	wg.Wait()
	totalDuration := time.Since(startTime)

	// Get combined metrics
	combinedMetrics := metricsCollector.CombinedStats()
	totalSuccess := metricsCollector.GetSuccessCount()
	totalErrors := metricsCollector.GetErrorCount()

	log.Println("Invert read test completed:")
	log.Printf("  Total duration: %v", totalDuration)
	log.Printf("  Total queries: %d", totalSuccess+totalErrors)
	log.Printf("  Successful queries: %d", totalSuccess)
	log.Printf("  Failed queries: %d", totalErrors)
	log.Printf("  Success rate: %.2f%%", float64(totalSuccess)*100/float64(totalSuccess+totalErrors))
	log.Printf("  Throughput: %.2f queries/sec", float64(totalSuccess)/totalDuration.Seconds())
	log.Printf("  Latency metrics: %s", combinedMetrics.String())
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
	flag.StringVar(&testType, "test", "setup", "Test type to run: setup, setupindex, write-vertex, write-edge, relation, all")
	flag.IntVar(&startZone, "start-zone", ZONE_START, "Start zone ID for edge test")
	flag.IntVar(&endZone, "end-zone", ZONE_START+ZONES_TOTAL, "End zone ID for edge test")
	flag.IntVar(&batchNum, "batch-num", EDGES_PER_RELATION, "Number of edges per batch for edge write test")
	flag.Parse()

	ctx := context.Background()

	// Fully-qualified database name:
	//   projects/{PROJECT}/instances/{INSTANCE}/databases/{DATABASE}
	dbPath := fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instanceID, databaseID)

	cfg := spanner.ClientConfig{
		SessionPoolConfig: spanner.SessionPoolConfig{
			MinOpened: 1600,
			MaxOpened: 3200,
		},
		NumChannels: 16,
	}

	// If you set GOOGLE_APPLICATION_CREDENTIALS, you can omit the option.
	client, err := spanner.NewClientWithConfig(ctx, dbPath, cfg,
		option.WithCredentialsFile(credentialsFile))
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

	case "setupindex":
		log.Println("Running setup indexes test...")
		if err := setupAllTableIndexes(ctx); err != nil {
			log.Fatalf("Failed to setup indexes: %v", err)
		}

	case "write-vertex":
		log.Println("Running vertex write test...")
		if BATCH_NUM > 1 {
			log.Printf("Using batch mode with batch size %d", BATCH_NUM)
			spannerWriteBatchVertexTest(client, BATCH_NUM)
		} else {
			log.Println("Using single insert mode")
			spannerWriteVertexTest(client, PreGenerateVertexData)
		}

	case "write-edge":
		log.Printf("Running edge write test for zones [%d, %d)...", startZone, endZone)
		spannerWriteEdgeTest(client, startZone, endZone, batchNum)

	case "update-vertex":
		log.Println("Running vertex update test...")
		spannerUpdateVertexTest(client)

	case "update-edge":
		log.Println("Running edge update test...")
		spannerUpdateEdgeTest(client)

	case "read-invert":
		log.Println("Running invert read test...")
		spannerReadInvertTest(client)

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
		log.Println("Available test types: setup, setupindex, write-vertex, write-edge,update-vertex, relation, all")
		os.Exit(1)
	}

	log.Println("Benchmark completed")
}
