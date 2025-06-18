package main

import (
	"context"
	"fmt"
	"log"
	"strings"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"google.golang.org/api/option"
	databasepb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
	"google.golang.org/grpc/status"
)

const (
	projectID  = "superb-receiver-463215-f7"
	instanceID = "graph-demo"
	databaseID = "mtbench"
	graphName  = "graph0618"
)

// setupGraphSpanner creates all tables, indexes, TTL policies, and the property graph.
func setupGraphSpanner(ctx context.Context) error {
	admin, err := database.NewDatabaseAdminClient(ctx)
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

	// 2. Vertex table
	ddl = append(ddl, `
CREATE TABLE Users (
  uid          INT64  NOT NULL,
  attr11       INT64,
  attr12       INT64,
  attr13       INT64,
  expire_time  TIMESTAMP OPTIONS (allow_commit_timestamp=true)
) PRIMARY KEY (uid)`)

	ddl = append(ddl,
		`CREATE INDEX user_attr11_attr12_attr13_idx
		   ON Users(attr11, attr12, attr13)`,
		`ALTER TABLE Users
		   ALTER ROW DELETION POLICY (OLDER_THAN(expire_time, INTERVAL 8 DAY))`,
	)

	// 3. Edge tables + indexes + TTL
	edgeLabels := []string{"Rel1", "Rel2", "Rel3", "Rel4", "Rel5"}
	for _, label := range edgeLabels {
		ddl = append(ddl, fmt.Sprintf(`
CREATE TABLE %s (
  edge_id      STRING(36) NOT NULL,
  src_uid      INT64      NOT NULL,
  dst_uid      INT64      NOT NULL,
  attr101      INT64,
  attr102      INT64,
  attr103      INT64,
  expire_time  TIMESTAMP OPTIONS (allow_commit_timestamp=true)
) PRIMARY KEY (edge_id)`, label))

		ddl = append(ddl, fmt.Sprintf(
			`CREATE INDEX %s_attr101_attr102_attr103_idx
			   ON %s(attr101, attr102, attr103)`,
			strings.ToLower(label), label))

		ddl = append(ddl, fmt.Sprintf(
			`ALTER TABLE %s
			   ALTER ROW DELETION POLICY (OLDER_THAN(expire_time, INTERVAL 8 DAY))`,
			label))
	}

	// 4. Property graph definition
	var edgeDefs []string
	for _, l := range edgeLabels {
		edgeDefs = append(edgeDefs, fmt.Sprintf(`
  %s
    SOURCE KEY (src_uid) REFERENCES Users(uid)
    DESTINATION KEY (dst_uid) REFERENCES Users(uid)
    LABEL %s PROPERTIES (attr101, attr102, attr103)`, l, l))
	}
	graphDDL := fmt.Sprintf(`CREATE PROPERTY GRAPH %s
NODE TABLES (
  Users KEY (uid)
    LABEL User PROPERTIES (uid, attr11, attr12, attr13)
)
EDGE TABLES (%s
)`, graphName, strings.Join(edgeDefs, ","))

	ddl = append(ddl, graphDDL)

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

func main() {
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

	setupGraphSpanner(ctx)
}
