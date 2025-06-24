package main

import (
	"fmt"
	"log"
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

	db, err := internal.NewDatabase("my_db")
	if err != nil {
		if _, ok := err.(*internal.DatabaseDoesNotExistError); ok {
			db, err = internal.CreateDatabase("my_db")
			if err != nil {
				log.Fatal(err)
			}
		} else {
			log.Fatal(err)
		}
	}
	defer db.Close()

	createTable(db)
	insert(db, 1, "software engineer", 31, true)
	insert(db, 2, "software engineer", 27, false)
	insert(db, 3, "designer", 28, true)
	queryAll(db)
}

func testReadTable() {
	db, err := internal.NewDatabase("my_db")
	if err != nil {
		if _, ok := err.(*internal.DatabaseDoesNotExistError); ok {
			db, err = internal.CreateDatabase("my_db")
			if err != nil {
				log.Fatal(err)
			}
		} else {
			log.Fatal(err)
		}
	}
	defer db.Close()

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

	age, err := column.New("age", types.TypeByte, column.Opts{})
	if err != nil {
		log.Fatal(err)
	}

	job, err := column.New("job", types.TypeString, column.Opts{})
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
	fmt.Printf("%s\n", rows.Extra)
}

func queryByJob(db *internal.Database, job string) {
	rows, err := db.Tables["users"].Select(map[string]interface{}{
		"job": job,
	})
	if err != nil {
		log.Fatal(err)
	}
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
			"id":        int64(i),
			"username":  "user" + strconv.Itoa(i),
			"job":       "software engineer",
			"age":       byte(30),
			"is_active": true,
		}, true)
		if err != nil {
			log.Fatal(err)
		}
	}
	fmt.Println("seeding done")
}

func measureOne(db *internal.Database, id int64) {
	start := time.Now()
	query(db, id)
	since := time.Since(start)
	fmt.Printf("selected ID %d in %d microseconds\n", id, since.Microseconds())
}
