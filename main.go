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
