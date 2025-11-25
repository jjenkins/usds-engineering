package model

import (
	"database/sql"
	"time"
)

// Agency represents a federal agency that issues regulations
type Agency struct {
	ID              int
	AgencyName      string
	ShortName       sql.NullString
	Slug            string
	ParentID        sql.NullInt64
	TotalWordCount  int
	RegulationCount int
	Checksum        string
	UpdatedAt       time.Time
}

// AgencySnapshot represents a historical snapshot of agency metrics
type AgencySnapshot struct {
	ID              int
	AgencyID        int
	AgencyName      string
	TotalWordCount  int
	RegulationCount int
	Checksum        string
	SnapshotDate    time.Time
	CreatedAt       time.Time
}

// AgencyMeta represents agency data from the eCFR Admin API
type AgencyMeta struct {
	Name          string
	ShortName     string
	Slug          string
	Children      []AgencyMeta
	CFRReferences []CFRReference
}

// CFRReference represents a reference to a CFR title/chapter
type CFRReference struct {
	Title   int
	Chapter string
}

// AgencyTitle represents the many-to-many relationship between agencies and titles
type AgencyTitle struct {
	AgencyID    int
	TitleNumber int
}
