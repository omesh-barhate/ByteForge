package main

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"time"

	"github.com/omesh-barhate/ByteForge/internal"
	"github.com/omesh-barhate/ByteForge/internal/platform/types"
	"github.com/omesh-barhate/ByteForge/internal/table/column"
)

func main() {
	if len(os.Args) == 1 {
		testCreateTable()
		os.Exit(0)
	}

	cmd := os.Args[1]
	switch cmd {
	case "c":
		testCreateTable()
	case "r":
		testReadTable()
	}
}

func testCreateTable() {
	_ = os.Remove("./data/my_db/users.bin")
	_ = os.Remove("./data/my_db/users_wal.bin")
	_ = os.Remove("./data/my_db/users_wal_last_commit.bin")
	db, err := internal.NewDatabase("my_db")
	if err != nil {
		db, err = internal.CreateDatabase("my_db")
		if err != nil {
			log.Fatal(err)
		}
	}

	createTable(db)
	insertOne(db, 1)
	insertOne(db, 2)
	insertOne(db, 3)
	queryAll(db)
}

func testReadTable() {
	db, err := internal.NewDatabase("my_db")
	if err != nil {
		log.Fatal(err)
	}

	insertOne(db, 10)
	queryAll(db)
}

func createTable(db *internal.Database) {
	id, err := column.New("id", types.TypeInt64, column.NewOpts(false))
	if err != nil {
		log.Fatal(err)
	}

	username, err := column.New("username", types.TypeString, column.NewOpts(false))
	if err != nil {
		log.Fatal(err)
	}

	age, err := column.New("age", types.TypeByte, column.NewOpts(true))
	if err != nil {
		log.Fatal(err)
	}

	job, err := column.New("job", types.TypeString, column.NewOpts(true))
	if err != nil {
		log.Fatal(err)
	}

	isCool, err := column.New("is_cool", types.TypeBool, column.NewOpts(true))
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.CreateTable("users", []string{"id", "username", "age", "job", "is_cool"}, map[string]*column.Column{
		"id":       id,
		"username": username,
		"age":      age,
		"job":      job,
		"is_cool":  isCool,
	})
	if err != nil {
		log.Fatal(err)
	}
}

func insertOne(db *internal.Database, id int64) {
	var job string
	if id == 1 || id == 2 {
		job = "software engineer"
	} else {
		job = "musician"
	}
	age := rand.Intn(10) + 20
	uname := fmt.Sprintf("user%d", id)
	_, err := db.Tables["users"].Insert(map[string]interface{}{
		"id":       id,
		"username": uname,
		"age":      byte(age),
		"job":      job,
		"is_cool":  true,
	}, true)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("%s inserted\n", uname)
}

func update(db *internal.Database) {
	_, err := db.Tables["users"].Update(map[string]interface{}{
		"job": "software engineer",
	}, map[string]interface{}{
		"job": "developer",
	})

	if err != nil {
		log.Fatal(err)
	}
}

func printRaw(db *internal.Database) {
	b, _ := db.Tables["users"].ReadRaw()
	fmt.Println(b)
}

func printRawWAL(db *internal.Database) {
	b, _ := db.Tables["users"].ReadRawWAL()
	fmt.Println(b)
}

func printRawWALLastID(db *internal.Database) {
	b, _ := db.Tables["users"].ReadRawLastIDWAL()
	fmt.Println(b)
}

func del(db *internal.Database) {
	_, err := db.Tables["users"].Delete(map[string]interface{}{
		"id": int64(2),
	})

	if err != nil {
		fmt.Println("here")
		log.Fatal(err)
	}
}

func queryAll(db *internal.Database) {
	start := time.Now()
	rows, err := db.Tables["users"].Select(map[string]interface{}{})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(rows)
	elapsed := time.Since(start)
	fmt.Printf("selected %d records in %dms\n", len(rows), elapsed.Milliseconds())
}

func query(db *internal.Database, id int64) {
	start := time.Now()
	rows, err := db.Tables["users"].Select(map[string]interface{}{
		"id": id,
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(rows)
	elapsed := time.Since(start)
	fmt.Printf("selected %d records in %dms\n", len(rows), elapsed.Milliseconds())
}

func insertMany(db *internal.Database, n int) {
	start := time.Now()
	for i := 0; i < n; i++ {
		_, err := db.Tables["users"].Insert(map[string]interface{}{
			"id":       int64(i),
			"username": "user" + strconv.Itoa(i),
			"age":      byte(30),
			"job":      "software engineer",
			"is_cool":  true,
		}, true)
		if err != nil {
			log.Fatal(err)
		}
	}

	elapsed := time.Since(start)
	fmt.Printf("Inserted %d records in %dms\n", n, elapsed.Milliseconds())
}
