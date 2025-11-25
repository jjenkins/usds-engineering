package store

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"time"

	"github.com/jjenkins/usds/internal/model"
)

// TitleWithDensity wraps a Title with its percentile-based density score
type TitleWithDensity struct {
	model.Title
	DensityScore float64 // Percentile rank (0.0 = least dense, 1.0 = most dense)
}

// calculateTitleDensityScores computes percentile-based density scores for all titles
func calculateTitleDensityScores(titles []TitleWithDensity) {
	type densityInfo struct {
		index   int
		density float64
	}
	var densities []densityInfo

	for i := range titles {
		if titles[i].SectionCount > 0 {
			density := float64(titles[i].WordCount) / float64(titles[i].SectionCount)
			densities = append(densities, densityInfo{index: i, density: density})
		}
	}

	if len(densities) == 0 {
		return
	}

	// Sort by density ascending
	sort.Slice(densities, func(i, j int) bool {
		return densities[i].density < densities[j].density
	})

	// Assign percentile scores
	n := len(densities)
	for rank, d := range densities {
		titles[d.index].DensityScore = float64(rank) / float64(n-1)
		if n == 1 {
			titles[d.index].DensityScore = 0.5
		}
	}
}

// TitleStore handles database operations for titles
type TitleStore struct {
	db *sql.DB
}

// NewTitleStore creates a new TitleStore
func NewTitleStore(db *sql.DB) *TitleStore {
	return &TitleStore{db: db}
}

// GetByNumber retrieves a title by its number
func (s *TitleStore) GetByNumber(ctx context.Context, titleNumber int) (*model.Title, error) {
	query := `
		SELECT id, title_number, title_name, word_count, section_count,
		       checksum, last_amended_date, fetched_at, created_at
		FROM titles
		WHERE title_number = $1
	`

	var t model.Title
	err := s.db.QueryRowContext(ctx, query, titleNumber).Scan(
		&t.ID,
		&t.TitleNumber,
		&t.TitleName,
		&t.WordCount,
		&t.SectionCount,
		&t.Checksum,
		&t.LastAmendedDate,
		&t.FetchedAt,
		&t.CreatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get title %d: %w", titleNumber, err)
	}

	return &t, nil
}

// UpsertTitle inserts or updates a title
func (s *TitleStore) UpsertTitle(ctx context.Context, t *model.Title) error {
	query := `
		INSERT INTO titles (title_number, title_name, word_count, section_count,
		                    checksum, last_amended_date, fetched_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (title_number) DO UPDATE SET
			title_name = EXCLUDED.title_name,
			word_count = EXCLUDED.word_count,
			section_count = EXCLUDED.section_count,
			checksum = EXCLUDED.checksum,
			last_amended_date = EXCLUDED.last_amended_date,
			fetched_at = EXCLUDED.fetched_at
		RETURNING id
	`

	err := s.db.QueryRowContext(ctx, query,
		t.TitleNumber,
		t.TitleName,
		t.WordCount,
		t.SectionCount,
		t.Checksum,
		t.LastAmendedDate,
		t.FetchedAt,
	).Scan(&t.ID)

	if err != nil {
		return fmt.Errorf("failed to upsert title %d: %w", t.TitleNumber, err)
	}

	return nil
}

// InsertSnapshot inserts a title snapshot
func (s *TitleStore) InsertSnapshot(ctx context.Context, snap *model.TitleSnapshot) error {
	query := `
		INSERT INTO title_snapshots (title_number, title_name, word_count,
		                             section_count, checksum, last_amended_date, snapshot_date)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (title_number, snapshot_date) DO UPDATE SET
			title_name = EXCLUDED.title_name,
			word_count = EXCLUDED.word_count,
			section_count = EXCLUDED.section_count,
			checksum = EXCLUDED.checksum,
			last_amended_date = EXCLUDED.last_amended_date
		RETURNING id
	`

	err := s.db.QueryRowContext(ctx, query,
		snap.TitleNumber,
		snap.TitleName,
		snap.WordCount,
		snap.SectionCount,
		snap.Checksum,
		snap.LastAmendedDate,
		snap.SnapshotDate,
	).Scan(&snap.ID)

	if err != nil {
		return fmt.Errorf("failed to insert snapshot for title %d: %w", snap.TitleNumber, err)
	}

	return nil
}

// SaveTitleWithSnapshot saves the current title and only creates a snapshot if content changed
func (s *TitleStore) SaveTitleWithSnapshot(ctx context.Context, t *model.Title, snapshotDate time.Time) (changed bool, err error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Check if there's already a snapshot for this exact date with the same checksum
	// This allows re-imports of the same date to be idempotent, while ensuring
	// historical imports for different dates always create snapshots
	var existingChecksum sql.NullString
	checksumQuery := `
		SELECT checksum FROM title_snapshots
		WHERE title_number = $1 AND snapshot_date = $2
	`
	tx.QueryRowContext(ctx, checksumQuery, t.TitleNumber, snapshotDate).Scan(&existingChecksum)

	// Create snapshot if: no snapshot exists for this date, OR checksum differs (re-import with changes)
	changed = !existingChecksum.Valid || existingChecksum.String != t.Checksum

	// Upsert title (always update current state)
	upsertQuery := `
		INSERT INTO titles (title_number, title_name, word_count, section_count,
		                    checksum, last_amended_date, fetched_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (title_number) DO UPDATE SET
			title_name = EXCLUDED.title_name,
			word_count = EXCLUDED.word_count,
			section_count = EXCLUDED.section_count,
			checksum = EXCLUDED.checksum,
			last_amended_date = EXCLUDED.last_amended_date,
			fetched_at = EXCLUDED.fetched_at
		RETURNING id
	`

	err = tx.QueryRowContext(ctx, upsertQuery,
		t.TitleNumber,
		t.TitleName,
		t.WordCount,
		t.SectionCount,
		t.Checksum,
		t.LastAmendedDate,
		t.FetchedAt,
	).Scan(&t.ID)
	if err != nil {
		return false, fmt.Errorf("failed to upsert title %d: %w", t.TitleNumber, err)
	}

	// Only insert snapshot if content changed
	if changed {
		snapshotQuery := `
			INSERT INTO title_snapshots (title_number, title_name, word_count,
			                             section_count, checksum, last_amended_date, snapshot_date)
			VALUES ($1, $2, $3, $4, $5, $6, $7)
			ON CONFLICT (title_number, snapshot_date) DO UPDATE SET
				title_name = EXCLUDED.title_name,
				word_count = EXCLUDED.word_count,
				section_count = EXCLUDED.section_count,
				checksum = EXCLUDED.checksum,
				last_amended_date = EXCLUDED.last_amended_date
		`

		_, err = tx.ExecContext(ctx, snapshotQuery,
			t.TitleNumber,
			t.TitleName,
			t.WordCount,
			t.SectionCount,
			t.Checksum,
			t.LastAmendedDate,
			snapshotDate,
		)
		if err != nil {
			return false, fmt.Errorf("failed to insert snapshot for title %d: %w", t.TitleNumber, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return changed, nil
}

// GetAll retrieves all titles ordered by title number (excludes full_content for performance)
func (s *TitleStore) GetAll(ctx context.Context) ([]model.Title, error) {
	query := `
		SELECT id, title_number, title_name, word_count, section_count,
		       checksum, last_amended_date, fetched_at, created_at
		FROM titles
		ORDER BY title_number
	`

	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to get titles: %w", err)
	}
	defer rows.Close()

	var titles []model.Title
	for rows.Next() {
		var t model.Title
		err := rows.Scan(
			&t.ID,
			&t.TitleNumber,
			&t.TitleName,
			&t.WordCount,
			&t.SectionCount,
			&t.Checksum,
			&t.LastAmendedDate,
			&t.FetchedAt,
			&t.CreatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan title: %w", err)
		}
		titles = append(titles, t)
	}

	return titles, rows.Err()
}

// GetAllSorted retrieves all titles with custom sorting (excludes full_content for performance)
func (s *TitleStore) GetAllSorted(ctx context.Context, sortBy, order string) ([]model.Title, error) {
	// Whitelist valid sort columns to prevent SQL injection
	validColumns := map[string]string{
		"number":        "title_number",
		"name":          "title_name",
		"word_count":    "word_count",
		"section_count": "section_count",
		"last_amended":  "last_amended_date",
	}

	column, ok := validColumns[sortBy]
	if !ok {
		column = "title_number"
	}

	sortOrder := "ASC"
	if order == "desc" {
		sortOrder = "DESC"
	}

	query := fmt.Sprintf(`
		SELECT id, title_number, title_name, word_count, section_count,
		       checksum, last_amended_date, fetched_at, created_at
		FROM titles
		ORDER BY %s %s
	`, column, sortOrder)

	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to get titles: %w", err)
	}
	defer rows.Close()

	var titles []model.Title
	for rows.Next() {
		var t model.Title
		err := rows.Scan(
			&t.ID,
			&t.TitleNumber,
			&t.TitleName,
			&t.WordCount,
			&t.SectionCount,
			&t.Checksum,
			&t.LastAmendedDate,
			&t.FetchedAt,
			&t.CreatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan title: %w", err)
		}
		titles = append(titles, t)
	}

	return titles, rows.Err()
}

// GetSnapshots retrieves all snapshots for a title ordered by date descending
func (s *TitleStore) GetSnapshots(ctx context.Context, titleNumber int) ([]model.TitleSnapshot, error) {
	query := `
		SELECT id, title_number, title_name, word_count, section_count,
		       checksum, last_amended_date, snapshot_date, created_at
		FROM title_snapshots
		WHERE title_number = $1
		ORDER BY snapshot_date DESC
	`

	rows, err := s.db.QueryContext(ctx, query, titleNumber)
	if err != nil {
		return nil, fmt.Errorf("failed to get snapshots for title %d: %w", titleNumber, err)
	}
	defer rows.Close()

	var snapshots []model.TitleSnapshot
	for rows.Next() {
		var snap model.TitleSnapshot
		err := rows.Scan(
			&snap.ID,
			&snap.TitleNumber,
			&snap.TitleName,
			&snap.WordCount,
			&snap.SectionCount,
			&snap.Checksum,
			&snap.LastAmendedDate,
			&snap.SnapshotDate,
			&snap.CreatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan snapshot: %w", err)
		}
		snapshots = append(snapshots, snap)
	}

	return snapshots, rows.Err()
}

// GetAgenciesForTitle retrieves all agencies linked to a title
func (s *TitleStore) GetAgenciesForTitle(ctx context.Context, titleNumber int) ([]model.Agency, error) {
	query := `
		SELECT a.id, a.agency_name, a.short_name, a.slug, a.parent_id,
		       a.total_word_count, a.regulation_count, a.checksum, a.updated_at
		FROM agencies a
		INNER JOIN agency_titles at ON a.id = at.agency_id
		WHERE at.title_number = $1
		ORDER BY a.agency_name
	`

	rows, err := s.db.QueryContext(ctx, query, titleNumber)
	if err != nil {
		return nil, fmt.Errorf("failed to get agencies for title %d: %w", titleNumber, err)
	}
	defer rows.Close()

	var agencies []model.Agency
	for rows.Next() {
		var a model.Agency
		err := rows.Scan(
			&a.ID,
			&a.AgencyName,
			&a.ShortName,
			&a.Slug,
			&a.ParentID,
			&a.TotalWordCount,
			&a.RegulationCount,
			&a.Checksum,
			&a.UpdatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan agency: %w", err)
		}
		agencies = append(agencies, a)
	}

	return agencies, rows.Err()
}

// CountTitles returns the total number of titles
func (s *TitleStore) CountTitles(ctx context.Context) (int, error) {
	var count int
	err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM titles").Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to count titles: %w", err)
	}
	return count, nil
}

// GetTotalWordCount returns the sum of all word counts
func (s *TitleStore) GetTotalWordCount(ctx context.Context) (int, error) {
	var total int
	err := s.db.QueryRowContext(ctx, "SELECT COALESCE(SUM(word_count), 0) FROM titles").Scan(&total)
	if err != nil {
		return 0, fmt.Errorf("failed to get total word count: %w", err)
	}
	return total, nil
}

// GetAverageDensity returns the average regulatory density (words per section)
func (s *TitleStore) GetAverageDensity(ctx context.Context) (float64, error) {
	var avg float64
	query := `SELECT COALESCE(AVG(CASE WHEN section_count > 0 THEN word_count::float / section_count ELSE 0 END), 0) FROM titles`
	err := s.db.QueryRowContext(ctx, query).Scan(&avg)
	if err != nil {
		return 0, fmt.Errorf("failed to get average density: %w", err)
	}
	return avg, nil
}

// GetAllSortedWithDensity retrieves all titles with density scores
func (s *TitleStore) GetAllSortedWithDensity(ctx context.Context, sortBy, order string) ([]TitleWithDensity, error) {
	// Whitelist valid sort columns to prevent SQL injection
	validColumns := map[string]string{
		"number":        "title_number",
		"name":          "title_name",
		"word_count":    "word_count",
		"section_count": "section_count",
		"last_amended":  "last_amended_date",
	}

	column, ok := validColumns[sortBy]
	if !ok {
		column = "title_number"
	}

	sortOrder := "ASC"
	if order == "desc" {
		sortOrder = "DESC"
	}

	query := fmt.Sprintf(`
		SELECT id, title_number, title_name, word_count, section_count,
		       checksum, last_amended_date, fetched_at, created_at
		FROM titles
		ORDER BY %s %s
	`, column, sortOrder)

	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to get titles: %w", err)
	}
	defer rows.Close()

	var titles []TitleWithDensity
	for rows.Next() {
		var t TitleWithDensity
		err := rows.Scan(
			&t.ID,
			&t.TitleNumber,
			&t.TitleName,
			&t.WordCount,
			&t.SectionCount,
			&t.Checksum,
			&t.LastAmendedDate,
			&t.FetchedAt,
			&t.CreatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan title: %w", err)
		}
		titles = append(titles, t)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	// Calculate percentile-based density scores
	calculateTitleDensityScores(titles)

	return titles, nil
}

// GetDensityScoreForTitle calculates the percentile-based density score for a single title
func (s *TitleStore) GetDensityScoreForTitle(ctx context.Context, title *model.Title) (float64, error) {
	if title.SectionCount == 0 {
		return 0, nil
	}

	titleDensity := float64(title.WordCount) / float64(title.SectionCount)

	// Count how many titles have lower density
	query := `
		SELECT COUNT(*) FROM titles
		WHERE section_count > 0
		AND (word_count::float / section_count::float) < $1
	`
	var lowerCount int
	if err := s.db.QueryRowContext(ctx, query, titleDensity).Scan(&lowerCount); err != nil {
		return 0, err
	}

	// Count total titles with density
	var totalCount int
	if err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM titles WHERE section_count > 0").Scan(&totalCount); err != nil {
		return 0, err
	}

	if totalCount <= 1 {
		return 0.5, nil
	}

	return float64(lowerCount) / float64(totalCount-1), nil
}

// GetSnapshotDates returns all unique snapshot dates
func (s *TitleStore) GetSnapshotDates(ctx context.Context) ([]time.Time, error) {
	query := `SELECT DISTINCT snapshot_date FROM title_snapshots ORDER BY snapshot_date DESC`
	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to get snapshot dates: %w", err)
	}
	defer rows.Close()

	var dates []time.Time
	for rows.Next() {
		var date time.Time
		if err := rows.Scan(&date); err != nil {
			return nil, fmt.Errorf("failed to scan date: %w", err)
		}
		dates = append(dates, date)
	}

	return dates, rows.Err()
}
