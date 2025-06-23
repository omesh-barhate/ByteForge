package table

import (
	"testing"
)

func TestFindContainingPageFirst(t *testing.T) {
	pagePositions := []int64{430, 558, 686}
	var pos int64 = 533
	got, _ := findContainingPage(pagePositions, pos)
	var want int64 = 430
	if got != want {
		t.Errorf("findContainingPage() = %d, want = %d", got, want)
	}
}

func TestFindContainingPageMiddle(t *testing.T) {
	pagePositions := []int64{430, 558, 686}
	var pos int64 = 601
	got, _ := findContainingPage(pagePositions, pos)
	var want int64 = 558
	if got != want {
		t.Errorf("findContainingPage() = %d, want = %d", got, want)
	}
}

func TestFindContainingPageLast(t *testing.T) {
	pagePositions := []int64{430, 558, 686}
	var pos int64 = 702
	got, _ := findContainingPage(pagePositions, pos)
	var want int64 = 686
	if got != want {
		t.Errorf("findContainingPage() = %d, want = %d", got, want)
	}
}

func TestFindContainingPageNotFound(t *testing.T) {
	pagePositions := []int64{430, 558, 686}
	var pos int64 = 100
	_, err := findContainingPage(pagePositions, pos)
	if err == nil {
		t.Errorf("findContainingPage() = %v, want = PageNotFoundError", err)
	}
}

func TestFindContainingPageEmpty(t *testing.T) {
	var pagePositions []int64
	var pos int64 = 100
	_, err := findContainingPage(pagePositions, pos)
	if err == nil {
		t.Errorf("findContainingPage() = %v, want = PageNotFoundError", err)
	}
}
