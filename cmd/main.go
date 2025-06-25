package main

import (
	"fmt"
	"log"
	"os"
	"strconv"

	"github.com/omesh-barhate/ByteForge/internal"
	"github.com/omesh-barhate/ByteForge/internal/platform/parser/encoding"
	"github.com/omesh-barhate/ByteForge/internal/platform/types"
	"github.com/omesh-barhate/ByteForge/internal/table/column"
	"github.com/omesh-barhate/ByteForge/internal/table/fulltext"
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
	case "t":
		testFoo()
	}
}

func testFoo() {
	values := []encoding.EmbeddedValueMarshaler{
		fulltext.NewIndexItem(128, 1),
		fulltext.NewIndexItem(128, 2),
	}
	m := encoding.NewListMarshaler(values)
	data, err := m.MarshalBinary()
	if err != nil {
		panic(err)
	}
	fmt.Printf("%v\n", data)
}

func testCreateTable() {
	_ = os.Remove("./data/my_db/users.bin")
	_ = os.Remove("./data/my_db/users_idx.bin")
	_ = os.Remove("./data/my_db/users_fulltext_idx.bin")
	_ = os.Remove("./data/my_db/users_wal.bin")
	_ = os.Remove("./data/my_db/users_wal_last_commit.bin")

	db, err := internal.NewDatabase("my_db")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	createTable(db)

	insert(db, 1, "software engineer", 31, true)
	insert(db, 2, "software engineer", 27, false)
	insert(db, 3, "software engineer", 27, false)
	insert(db, 4, "designer", 28, true)

	printRawFulLTextIdx(db)
}

func testReadTable() {
	db, err := internal.NewDatabase("my_db")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	queryAll(db)
}

func createTable(db *internal.Database) {
	id, err := column.New("id", types.TypeInt64, column.NewColumnOpts(false, false))
	if err != nil {
		log.Fatal(err)
	}

	username, err := column.New("username", types.TypeString, column.NewColumnOpts(false, false))
	if err != nil {
		log.Fatal(err)
	}

	age, err := column.New("age", types.TypeByte, column.Opts{})
	if err != nil {
		log.Fatal(err)
	}

	job, err := column.New("job", types.TypeString, column.NewColumnOpts(false, true))
	if err != nil {
		log.Fatal(err)
	}

	isCool, err := column.New("is_active", types.TypeBool, column.Opts{})
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.CreateTable(db.Path, "users", []string{"id", "username", "age", "job", "is_active"}, map[string]*column.Column{
		"id":        id,
		"username":  username,
		"age":       age,
		"job":       job,
		"is_active": isCool,
	})
	if err != nil {
		log.Fatal(err)
	}
}

func insert(db *internal.Database, id int64, job string, age byte, isActive bool) {
	_, err := db.Tables["users"].Insert(map[string]interface{}{
		"id":        id,
		"username":  "user" + strconv.Itoa(int(id)),
		"age":       age,
		"job":       job,
		"is_active": isActive,
	}, true)
	if err != nil {
		log.Fatal(err)
	}
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

func printRawIdx(db *internal.Database) {
	b, _ := db.Tables["users"].ReadRawIdx()
	fmt.Println(b)
}

func printRawFulLTextIdx(db *internal.Database) {
	b, _ := db.Tables["users"].ReadRawFullTextIdx()
	fmt.Println(b)
}

func del(db *internal.Database) {
	_, err := db.Tables["users"].Delete(map[string]interface{}{
		"job": "designer",
	})

	if err != nil {
		fmt.Println("here")
		log.Fatal(err)
	}
}

func delById(db *internal.Database, id int64) {
	_, err := db.Tables["users"].Delete(map[string]interface{}{
		"id": id,
	})

	if err != nil {
		fmt.Println("here")
		log.Fatal(err)
	}
}

func query(db *internal.Database, id int64) {
	rows, err := db.Tables["users"].Select(map[string]interface{}{
		"id": id,
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(rows)
}

func queryByJob(db *internal.Database, job string) {
	rows, err := db.Tables["users"].Select(map[string]interface{}{
		"job": job,
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%#v\n", rows)
	fmt.Println(rows)
}

func queryAll(db *internal.Database) {
	rows, err := db.Tables["users"].Select(map[string]interface{}{})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(rows)
}

func seed(db *internal.Database, n int) {
	fmt.Println("seeding...")
	for i := 1; i < n; i++ {
		_, err := db.Tables["users"].Insert(map[string]interface{}{
			"id":       int64(i),
			"username": "user" + strconv.Itoa(i),
			"job":      "software engineer",
			"age":      byte(30),
			"is_cool":  true,
		}, true)
		if err != nil {
			log.Fatal(err)
		}
	}
	fmt.Println("seeding done")
}
