package internal

import (
	"log"
	"os"
	"testing"

	"github.com/omesh-barhate/ByteForge/internal/platform/types"
	"github.com/omesh-barhate/ByteForge/internal/table/column"
)

func TestInsert(t *testing.T) {
	db, err := CreateDatabase("test")
	if err != nil {
		panic(err)
	}
	defer removeDB()
	createTable(db)

	_, err = db.Tables["users"].Insert(map[string]interface{}{
		"id":        int64(1),
		"username":  "user1",
		"age":       byte(31),
		"job":       "software engineer",
		"is_active": true,
	}, true)
	if err != nil {
		t.Errorf("err should be nil: %v", err)
	}

	_, err = db.Tables["users"].Insert(map[string]interface{}{
		"id":        int64(2),
		"username":  "user2",
		"age":       byte(27),
		"job":       "software engineer",
		"is_active": false,
	}, true)
	if err != nil {
		t.Errorf("err should be nil: %v", err)
	}

	_, err = db.Tables["users"].Insert(map[string]interface{}{
		"id":        int64(3),
		"username":  "user3",
		"age":       byte(28),
		"job":       "designer",
		"is_active": true,
	}, true)
	if err != nil {
		t.Errorf("err should be nil: %v", err)
	}

	records, err := db.Tables["users"].Select(map[string]interface{}{})
	if err != nil {
		t.Errorf("err should be nil: %v", err)
	}

	if len(records) != 3 {
		t.Errorf("len(records) == %d, len(expected) == 3", len(records))
	}

	expected := []byte{99, 81, 0, 0, 0, 2, 64, 0, 0, 0, 105, 100, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 1, 0, 0, 0, 1, 4, 1, 0, 0, 0, 0, 99, 81, 0, 0, 0, 2, 64, 0, 0, 0, 117, 115, 101, 114, 110, 97, 109, 101, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 1, 0, 0, 0, 2, 4, 1, 0, 0, 0, 0, 99, 81, 0, 0, 0, 2, 64, 0, 0, 0, 97, 103, 101, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 1, 0, 0, 0, 3, 4, 1, 0, 0, 0, 0, 99, 81, 0, 0, 0, 2, 64, 0, 0, 0, 106, 111, 98, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 1, 0, 0, 0, 2, 4, 1, 0, 0, 0, 0, 99, 81, 0, 0, 0, 2, 64, 0, 0, 0, 105, 115, 95, 97, 99, 116, 105, 118, 101, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 1, 0, 0, 0, 4, 4, 1, 0, 0, 0, 0, 255, 124, 0, 0, 0, 100, 57, 0, 0, 0, 1, 8, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2, 5, 0, 0, 0, 117, 115, 101, 114, 49, 3, 1, 0, 0, 0, 31, 2, 17, 0, 0, 0, 115, 111, 102, 116, 119, 97, 114, 101, 32, 101, 110, 103, 105, 110, 101, 101, 114, 4, 1, 0, 0, 0, 1, 100, 57, 0, 0, 0, 1, 8, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 2, 5, 0, 0, 0, 117, 115, 101, 114, 50, 3, 1, 0, 0, 0, 27, 2, 17, 0, 0, 0, 115, 111, 102, 116, 119, 97, 114, 101, 32, 101, 110, 103, 105, 110, 101, 101, 114, 4, 1, 0, 0, 0, 0, 255, 53, 0, 0, 0, 100, 48, 0, 0, 0, 1, 8, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 2, 5, 0, 0, 0, 117, 115, 101, 114, 51, 3, 1, 0, 0, 0, 28, 2, 8, 0, 0, 0, 100, 101, 115, 105, 103, 110, 101, 114, 4, 1, 0, 0, 0, 1}
	b, err := db.Tables["users"].ReadRaw()
	if err != nil {
		t.Errorf("err should be nil: %v", err)
	}
	assertBytes(t, "table content", b, expected)

	expectedIdx := []byte{254, 93, 0, 0, 0, 253, 26, 0, 0, 0, 1, 8, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 8, 0, 0, 0, 174, 1, 0, 0, 0, 0, 0, 0, 253, 26, 0, 0, 0, 1, 8, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 1, 8, 0, 0, 0, 174, 1, 0, 0, 0, 0, 0, 0, 253, 26, 0, 0, 0, 1, 8, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 1, 8, 0, 0, 0, 47, 2, 0, 0, 0, 0, 0, 0}
	idx, err := db.Tables["users"].ReadRawIdx()
	if err != nil {
		t.Errorf("err should be nil: %v", err)
	}
	assertBytes(t, "index", idx, expectedIdx)
}

func TestUpdate(t *testing.T) {
	db, err := CreateDatabase("test")
	if err != nil {
		panic(err)
	}
	defer removeDB()
	createTable(db)

	_, err = db.Tables["users"].Insert(map[string]interface{}{
		"id":        int64(1),
		"username":  "user1",
		"age":       byte(31),
		"job":       "software engineer",
		"is_active": true,
	}, true)
	if err != nil {
		t.Errorf("err should be nil: %v", err)
	}

	_, err = db.Tables["users"].Insert(map[string]interface{}{
		"id":        int64(2),
		"username":  "user2",
		"age":       byte(27),
		"job":       "software engineer",
		"is_active": false,
	}, true)
	if err != nil {
		t.Errorf("err should be nil: %v", err)
	}

	_, err = db.Tables["users"].Insert(map[string]interface{}{
		"id":        int64(3),
		"username":  "user3",
		"age":       byte(28),
		"job":       "designer",
		"is_active": true,
	}, true)
	if err != nil {
		t.Errorf("err should be nil: %v", err)
	}

	_, err = db.Tables["users"].Update(map[string]interface{}{
		"job": "software engineer",
	}, map[string]interface{}{
		"job": "developer",
	})
	if err != nil {
		t.Errorf("err should be nil: %v", err)
	}

	records, err := db.Tables["users"].Select(map[string]interface{}{})
	if err != nil {
		t.Errorf("err should be nil: %v", err)
	}

	if len(records) != 3 {
		t.Errorf("len(records) == %d, len(expected) == 3", len(records))
	}

	expected := []byte{99, 81, 0, 0, 0, 2, 64, 0, 0, 0, 105, 100, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 1, 0, 0, 0, 1, 4, 1, 0, 0, 0, 0, 99, 81, 0, 0, 0, 2, 64, 0, 0, 0, 117, 115, 101, 114, 110, 97, 109, 101, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 1, 0, 0, 0, 2, 4, 1, 0, 0, 0, 0, 99, 81, 0, 0, 0, 2, 64, 0, 0, 0, 97, 103, 101, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 1, 0, 0, 0, 3, 4, 1, 0, 0, 0, 0, 99, 81, 0, 0, 0, 2, 64, 0, 0, 0, 106, 111, 98, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 1, 0, 0, 0, 2, 4, 1, 0, 0, 0, 0, 99, 81, 0, 0, 0, 2, 64, 0, 0, 0, 105, 115, 95, 97, 99, 116, 105, 118, 101, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 1, 0, 0, 0, 4, 4, 1, 0, 0, 0, 0, 255, 124, 0, 0, 0, 101, 57, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 101, 57, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 255, 107, 0, 0, 0, 100, 48, 0, 0, 0, 1, 8, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 2, 5, 0, 0, 0, 117, 115, 101, 114, 51, 3, 1, 0, 0, 0, 28, 2, 8, 0, 0, 0, 100, 101, 115, 105, 103, 110, 101, 114, 4, 1, 0, 0, 0, 1, 100, 49, 0, 0, 0, 1, 8, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2, 5, 0, 0, 0, 117, 115, 101, 114, 49, 3, 1, 0, 0, 0, 31, 2, 9, 0, 0, 0, 100, 101, 118, 101, 108, 111, 112, 101, 114, 4, 1, 0, 0, 0, 1, 255, 54, 0, 0, 0, 100, 49, 0, 0, 0, 1, 8, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 2, 5, 0, 0, 0, 117, 115, 101, 114, 50, 3, 1, 0, 0, 0, 27, 2, 9, 0, 0, 0, 100, 101, 118, 101, 108, 111, 112, 101, 114, 4, 1, 0, 0, 0, 0}
	b, err := db.Tables["users"].ReadRaw()
	if err != nil {
		t.Errorf("err should be nil: %v", err)
	}
	assertBytes(t, "table content", b, expected)

	expectedIdx := []byte{254, 93, 0, 0, 0, 253, 26, 0, 0, 0, 1, 8, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 8, 0, 0, 0, 47, 2, 0, 0, 0, 0, 0, 0, 253, 26, 0, 0, 0, 1, 8, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 1, 8, 0, 0, 0, 159, 2, 0, 0, 0, 0, 0, 0, 253, 26, 0, 0, 0, 1, 8, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 1, 8, 0, 0, 0, 47, 2, 0, 0, 0, 0, 0, 0}
	idx, err := db.Tables["users"].ReadRawIdx()
	if err != nil {
		t.Errorf("err should be nil: %v", err)
	}
	assertBytes(t, "index", idx, expectedIdx)
}

func TestDelete(t *testing.T) {
	db, err := CreateDatabase("test")
	if err != nil {
		panic(err)
	}
	defer removeDB()
	createTable(db)

	_, err = db.Tables["users"].Insert(map[string]interface{}{
		"id":        int64(1),
		"username":  "user1",
		"age":       byte(31),
		"job":       "software engineer",
		"is_active": true,
	}, true)
	if err != nil {
		t.Errorf("err should be nil: %v", err)
	}

	_, err = db.Tables["users"].Insert(map[string]interface{}{
		"id":        int64(2),
		"username":  "user2",
		"age":       byte(27),
		"job":       "software engineer",
		"is_active": false,
	}, true)
	if err != nil {
		t.Errorf("err should be nil: %v", err)
	}

	_, err = db.Tables["users"].Insert(map[string]interface{}{
		"id":        int64(3),
		"username":  "user3",
		"age":       byte(28),
		"job":       "designer",
		"is_active": true,
	}, true)
	if err != nil {
		t.Errorf("err should be nil: %v", err)
	}

	_, err = db.Tables["users"].Update(map[string]interface{}{
		"job": "software engineer",
	}, map[string]interface{}{
		"job": "developer",
	})
	if err != nil {
		t.Errorf("err should be nil: %v", err)
	}

	_, err = db.Tables["users"].Delete(map[string]interface{}{
		"job": "designer",
	})
	if err != nil {
		t.Errorf("err should be nil: %v", err)
	}

	records, err := db.Tables["users"].Select(map[string]interface{}{})
	if err != nil {
		t.Errorf("err should be nil: %v", err)
	}

	if len(records) != 2 {
		t.Errorf("len(records) == %d, len(expected) == 3", len(records))
	}

	expected := []byte{99, 81, 0, 0, 0, 2, 64, 0, 0, 0, 105, 100, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 1, 0, 0, 0, 1, 4, 1, 0, 0, 0, 0, 99, 81, 0, 0, 0, 2, 64, 0, 0, 0, 117, 115, 101, 114, 110, 97, 109, 101, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 1, 0, 0, 0, 2, 4, 1, 0, 0, 0, 0, 99, 81, 0, 0, 0, 2, 64, 0, 0, 0, 97, 103, 101, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 1, 0, 0, 0, 3, 4, 1, 0, 0, 0, 0, 99, 81, 0, 0, 0, 2, 64, 0, 0, 0, 106, 111, 98, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 1, 0, 0, 0, 2, 4, 1, 0, 0, 0, 0, 99, 81, 0, 0, 0, 2, 64, 0, 0, 0, 105, 115, 95, 97, 99, 116, 105, 118, 101, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 1, 0, 0, 0, 4, 4, 1, 0, 0, 0, 0, 255, 124, 0, 0, 0, 101, 57, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 101, 57, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 255, 107, 0, 0, 0, 101, 48, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 100, 49, 0, 0, 0, 1, 8, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2, 5, 0, 0, 0, 117, 115, 101, 114, 49, 3, 1, 0, 0, 0, 31, 2, 9, 0, 0, 0, 100, 101, 118, 101, 108, 111, 112, 101, 114, 4, 1, 0, 0, 0, 1, 255, 54, 0, 0, 0, 100, 49, 0, 0, 0, 1, 8, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 2, 5, 0, 0, 0, 117, 115, 101, 114, 50, 3, 1, 0, 0, 0, 27, 2, 9, 0, 0, 0, 100, 101, 118, 101, 108, 111, 112, 101, 114, 4, 1, 0, 0, 0, 0}
	b, err := db.Tables["users"].ReadRaw()
	if err != nil {
		t.Errorf("err should be nil: %v", err)
	}
	assertBytes(t, "table content", b, expected)

	expectedIdx := []byte{254, 62, 0, 0, 0, 253, 26, 0, 0, 0, 1, 8, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 8, 0, 0, 0, 47, 2, 0, 0, 0, 0, 0, 0, 253, 26, 0, 0, 0, 1, 8, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 1, 8, 0, 0, 0, 159, 2, 0, 0, 0, 0, 0, 0}
	idx, err := db.Tables["users"].ReadRawIdx()
	if err != nil {
		t.Errorf("err should be nil: %v", err)
	}
	assertBytes(t, "index", idx, expectedIdx)
}

func assertBytes(t *testing.T, funcName string, actual, expected []byte) {
	if len(actual) != len(expected) {
		t.Errorf("%s: len(actual) == %d, len(expected) == %d", funcName, len(actual), len(expected))
	}
	for i, _ := range actual {
		if actual[i] != expected[i] {
			t.Errorf("%s: actual[%d] == %d, expected[%d] == %d", funcName, i, actual[i], i, expected[i])
		}
	}
}

func createTable(db *Database) {
	id, err := column.New("id", types.TypeInt64, column.NewColumnOpts(false))
	if err != nil {
		log.Fatal(err)
	}

	username, err := column.New("username", types.TypeString, column.Opts{})
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

	isActive, err := column.New("is_active", types.TypeBool, column.Opts{})
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.CreateTable(db.Path, "users", []string{"id", "username", "age", "job", "is_active"}, map[string]*column.Column{
		"id":        id,
		"username":  username,
		"age":       age,
		"job":       job,
		"is_active": isActive,
	})
	if err != nil {
		log.Fatal(err)
	}
}

func removeDB() {
	err := os.RemoveAll("./data/test")
	if err != nil {
		log.Fatal(err)
	}
}
