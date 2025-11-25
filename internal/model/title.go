package model

import (
	"database/sql"
	"time"
)

// Title represents the current state of a CFR title
type Title struct {
	ID              int
	TitleNumber     int
	TitleName       string
	WordCount       int
	SectionCount    int
	Checksum        string
	LastAmendedDate sql.NullTime
	FetchedAt       time.Time
	CreatedAt       time.Time
}

// TitleSnapshot represents a historical snapshot of a CFR title
type TitleSnapshot struct {
	ID              int
	TitleNumber     int
	TitleName       string
	WordCount       int
	SectionCount    int
	Checksum        string
	LastAmendedDate sql.NullTime
	SnapshotDate    time.Time
	CreatedAt       time.Time
}

// TitleMeta represents metadata from the eCFR API titles list
type TitleMeta struct {
	Number          int
	Name            string
	LatestAmendedOn string
	LatestIssueDate string
	Reserved        bool
}
