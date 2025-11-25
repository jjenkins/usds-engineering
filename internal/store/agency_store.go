package store

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"time"

	"github.com/jjenkins/usds/internal/model"
)

// AgencyStore handles database operations for agencies
type AgencyStore struct {
	db *sql.DB
}

// NewAgencyStore creates a new AgencyStore
func NewAgencyStore(db *sql.DB) *AgencyStore {
	return &AgencyStore{db: db}
}

// GetBySlug retrieves an agency by its slug
func (s *AgencyStore) GetBySlug(ctx context.Context, slug string) (*model.Agency, error) {
	query := `
		SELECT id, agency_name, short_name, slug, parent_id, total_word_count,
		       regulation_count, checksum, updated_at
		FROM agencies
		WHERE slug = $1
	`

	var a model.Agency
	err := s.db.QueryRowContext(ctx, query, slug).Scan(
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
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get agency %s: %w", slug, err)
	}

	return &a, nil
}

// GetAll retrieves all agencies
func (s *AgencyStore) GetAll(ctx context.Context) ([]model.Agency, error) {
	query := `
		SELECT id, agency_name, short_name, slug, parent_id, total_word_count,
		       regulation_count, checksum, updated_at
		FROM agencies
		ORDER BY agency_name
	`

	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to get agencies: %w", err)
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

// UpsertAgency inserts or updates an agency, returns the ID
func (s *AgencyStore) UpsertAgency(ctx context.Context, a *model.Agency) error {
	query := `
		INSERT INTO agencies (agency_name, short_name, slug, parent_id, total_word_count,
		                      regulation_count, checksum, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT (slug) DO UPDATE SET
			agency_name = EXCLUDED.agency_name,
			short_name = EXCLUDED.short_name,
			parent_id = EXCLUDED.parent_id,
			total_word_count = EXCLUDED.total_word_count,
			regulation_count = EXCLUDED.regulation_count,
			checksum = EXCLUDED.checksum,
			updated_at = EXCLUDED.updated_at
		RETURNING id
	`

	err := s.db.QueryRowContext(ctx, query,
		a.AgencyName,
		a.ShortName,
		a.Slug,
		a.ParentID,
		a.TotalWordCount,
		a.RegulationCount,
		a.Checksum,
		time.Now(),
	).Scan(&a.ID)

	if err != nil {
		return fmt.Errorf("failed to upsert agency %s: %w", a.Slug, err)
	}

	return nil
}

// LinkAgencyTitle creates a link between an agency and a title
func (s *AgencyStore) LinkAgencyTitle(ctx context.Context, agencyID, titleNumber int) error {
	query := `
		INSERT INTO agency_titles (agency_id, title_number)
		VALUES ($1, $2)
		ON CONFLICT (agency_id, title_number) DO NOTHING
	`

	_, err := s.db.ExecContext(ctx, query, agencyID, titleNumber)
	if err != nil {
		return fmt.Errorf("failed to link agency %d to title %d: %w", agencyID, titleNumber, err)
	}

	return nil
}

// GetAgencyTitles retrieves all title numbers linked to an agency
func (s *AgencyStore) GetAgencyTitles(ctx context.Context, agencyID int) ([]int, error) {
	query := `SELECT title_number FROM agency_titles WHERE agency_id = $1`

	rows, err := s.db.QueryContext(ctx, query, agencyID)
	if err != nil {
		return nil, fmt.Errorf("failed to get agency titles: %w", err)
	}
	defer rows.Close()

	var titles []int
	for rows.Next() {
		var titleNum int
		if err := rows.Scan(&titleNum); err != nil {
			return nil, fmt.Errorf("failed to scan title number: %w", err)
		}
		titles = append(titles, titleNum)
	}

	return titles, rows.Err()
}

// GetChildrenIDs retrieves IDs of all child agencies
func (s *AgencyStore) GetChildrenIDs(ctx context.Context, parentID int) ([]int, error) {
	query := `SELECT id FROM agencies WHERE parent_id = $1`

	rows, err := s.db.QueryContext(ctx, query, parentID)
	if err != nil {
		return nil, fmt.Errorf("failed to get children: %w", err)
	}
	defer rows.Close()

	var ids []int
	for rows.Next() {
		var id int
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("failed to scan child id: %w", err)
		}
		ids = append(ids, id)
	}

	return ids, rows.Err()
}

// UpdateWordCount updates the word count and checksum for an agency
func (s *AgencyStore) UpdateWordCount(ctx context.Context, agencyID, wordCount, regulationCount int, checksum string) error {
	query := `
		UPDATE agencies
		SET total_word_count = $2, regulation_count = $3, checksum = $4, updated_at = $5
		WHERE id = $1
	`

	_, err := s.db.ExecContext(ctx, query, agencyID, wordCount, regulationCount, checksum, time.Now())
	if err != nil {
		return fmt.Errorf("failed to update word count for agency %d: %w", agencyID, err)
	}

	return nil
}

// InsertSnapshotIfChanged inserts an agency snapshot only if the checksum differs from the latest
// Also records which titles were linked at this snapshot point
func (s *AgencyStore) InsertSnapshotIfChanged(ctx context.Context, snap *model.AgencySnapshot, titleNumbers []int) (changed bool, err error) {
	// Check if there's already a snapshot for this exact date with the same checksum
	// This allows re-imports of the same date to be idempotent, while ensuring
	// historical imports for different dates always create snapshots
	var existingChecksum sql.NullString
	checksumQuery := `
		SELECT checksum FROM agency_snapshots
		WHERE agency_id = $1 AND snapshot_date = $2
	`
	s.db.QueryRowContext(ctx, checksumQuery, snap.AgencyID, snap.SnapshotDate).Scan(&existingChecksum)

	// Only skip if snapshot already exists for this date with same checksum
	if existingChecksum.Valid && existingChecksum.String == snap.Checksum {
		return false, nil
	}

	query := `
		INSERT INTO agency_snapshots (agency_id, agency_name, total_word_count, regulation_count,
		                              checksum, snapshot_date)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (agency_id, snapshot_date) DO UPDATE SET
			agency_name = EXCLUDED.agency_name,
			total_word_count = EXCLUDED.total_word_count,
			regulation_count = EXCLUDED.regulation_count,
			checksum = EXCLUDED.checksum
		RETURNING id
	`

	err = s.db.QueryRowContext(ctx, query,
		snap.AgencyID,
		snap.AgencyName,
		snap.TotalWordCount,
		snap.RegulationCount,
		snap.Checksum,
		snap.SnapshotDate,
	).Scan(&snap.ID)

	if err != nil {
		return false, fmt.Errorf("failed to insert snapshot for agency %d: %w", snap.AgencyID, err)
	}

	// Insert the title links for this snapshot
	for _, titleNum := range titleNumbers {
		linkQuery := `
			INSERT INTO agency_snapshot_titles (agency_snapshot_id, title_number)
			VALUES ($1, $2)
			ON CONFLICT (agency_snapshot_id, title_number) DO NOTHING
		`
		if _, err := s.db.ExecContext(ctx, linkQuery, snap.ID, titleNum); err != nil {
			// Log but don't fail the whole operation
			continue
		}
	}

	return true, nil
}

// ClearAgencyTitles removes all agency-title links (for re-import)
func (s *AgencyStore) ClearAgencyTitles(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, "DELETE FROM agency_titles")
	if err != nil {
		return fmt.Errorf("failed to clear agency_titles: %w", err)
	}
	return nil
}

// GetTitleWordCount retrieves the word count for a title
func (s *AgencyStore) GetTitleWordCount(ctx context.Context, titleNumber int) (int, error) {
	query := `SELECT word_count FROM titles WHERE title_number = $1`

	var wordCount int
	err := s.db.QueryRowContext(ctx, query, titleNumber).Scan(&wordCount)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("failed to get word count for title %d: %w", titleNumber, err)
	}

	return wordCount, nil
}

// AgencyWithDepth represents an agency with its hierarchical depth
type AgencyWithDepth struct {
	model.Agency
	Depth        int
	TitleCount   int
	DensityScore float64 // Percentile rank of density (0.0 = least dense, 1.0 = most dense)
}

// GetDensityScoreForAgency calculates the percentile-based density score for a single agency
func (s *AgencyStore) GetDensityScoreForAgency(ctx context.Context, agency *model.Agency) (float64, error) {
	if agency.RegulationCount == 0 {
		return 0, nil
	}

	agencyDensity := float64(agency.TotalWordCount) / float64(agency.RegulationCount)

	// Count how many agencies have lower density
	query := `
		SELECT COUNT(*) FROM agencies
		WHERE regulation_count > 0
		AND (total_word_count::float / regulation_count::float) < $1
	`
	var lowerCount int
	if err := s.db.QueryRowContext(ctx, query, agencyDensity).Scan(&lowerCount); err != nil {
		return 0, err
	}

	// Count total agencies with density
	var totalCount int
	if err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM agencies WHERE regulation_count > 0").Scan(&totalCount); err != nil {
		return 0, err
	}

	if totalCount <= 1 {
		return 0.5, nil // Only one agency
	}

	return float64(lowerCount) / float64(totalCount-1), nil
}

// calculateDensityScores computes percentile-based density scores for all agencies
func calculateDensityScores(agencies []AgencyWithDepth) {
	// Collect densities for agencies with titles
	type densityInfo struct {
		index   int
		density float64
	}
	var densities []densityInfo

	for i := range agencies {
		if agencies[i].TitleCount > 0 {
			density := float64(agencies[i].TotalWordCount) / float64(agencies[i].TitleCount)
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
		// Percentile rank: what fraction of agencies have lower density
		agencies[d.index].DensityScore = float64(rank) / float64(n-1)
		if n == 1 {
			agencies[d.index].DensityScore = 0.5 // Only one agency, middle score
		}
	}
}

// GetAllHierarchical retrieves all agencies with depth information for hierarchical display
func (s *AgencyStore) GetAllHierarchical(ctx context.Context) ([]AgencyWithDepth, error) {
	// Get all agencies
	agencies, err := s.GetAll(ctx)
	if err != nil {
		return nil, err
	}

	// Build a map for quick lookup
	agencyMap := make(map[int]*model.Agency)
	for i := range agencies {
		agencyMap[agencies[i].ID] = &agencies[i]
	}

	// Get title counts for all agencies
	titleCounts := make(map[int]int)
	countQuery := `SELECT agency_id, COUNT(*) FROM agency_titles GROUP BY agency_id`
	rows, err := s.db.QueryContext(ctx, countQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to get title counts: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var agencyID, count int
		if err := rows.Scan(&agencyID, &count); err != nil {
			return nil, fmt.Errorf("failed to scan title count: %w", err)
		}
		titleCounts[agencyID] = count
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// Calculate depth for each agency
	var calculateDepth func(a *model.Agency) int
	calculateDepth = func(a *model.Agency) int {
		if !a.ParentID.Valid {
			return 0
		}
		parent, ok := agencyMap[int(a.ParentID.Int64)]
		if !ok {
			return 0
		}
		return 1 + calculateDepth(parent)
	}

	// Build result with depths, sorting parents before children
	result := make([]AgencyWithDepth, 0, len(agencies))

	// First, add all top-level agencies
	var addAgencyWithChildren func(a *model.Agency, depth int)
	addAgencyWithChildren = func(a *model.Agency, depth int) {
		result = append(result, AgencyWithDepth{
			Agency:     *a,
			Depth:      depth,
			TitleCount: titleCounts[a.ID],
		})
		// Find and add children
		for i := range agencies {
			if agencies[i].ParentID.Valid && int(agencies[i].ParentID.Int64) == a.ID {
				addAgencyWithChildren(&agencies[i], depth+1)
			}
		}
	}

	// Start with top-level agencies (no parent)
	for i := range agencies {
		if !agencies[i].ParentID.Valid {
			addAgencyWithChildren(&agencies[i], 0)
		}
	}

	// Calculate percentile-based density scores
	calculateDensityScores(result)

	return result, nil
}

// GetAllSorted retrieves all agencies with custom sorting
func (s *AgencyStore) GetAllSorted(ctx context.Context, sortBy, order string) ([]AgencyWithDepth, error) {
	// For name sorting with ascending order, use hierarchical view (preserves parent-child structure)
	// For all other cases, use flat sorted list
	if sortBy == "name" && order == "asc" {
		return s.GetAllHierarchical(ctx)
	}

	return s.getAllSortedFlat(ctx, sortBy, order)
}

func (s *AgencyStore) getAllSortedFlat(ctx context.Context, sortBy, order string) ([]AgencyWithDepth, error) {
	sortOrder := "ASC"
	if order == "desc" {
		sortOrder = "DESC"
	}

	var query string
	if sortBy == "title_count" {
		query = fmt.Sprintf(`
			SELECT a.id, a.agency_name, a.short_name, a.slug, a.parent_id,
			       a.total_word_count, a.regulation_count, a.checksum, a.updated_at,
			       COUNT(at.title_number) as title_count
			FROM agencies a
			LEFT JOIN agency_titles at ON a.id = at.agency_id
			GROUP BY a.id
			ORDER BY title_count %s, a.agency_name ASC
		`, sortOrder)
	} else if sortBy == "name" {
		query = fmt.Sprintf(`
			SELECT a.id, a.agency_name, a.short_name, a.slug, a.parent_id,
			       a.total_word_count, a.regulation_count, a.checksum, a.updated_at,
			       (SELECT COUNT(*) FROM agency_titles WHERE agency_id = a.id) as title_count
			FROM agencies a
			ORDER BY a.agency_name %s
		`, sortOrder)
	} else {
		query = fmt.Sprintf(`
			SELECT a.id, a.agency_name, a.short_name, a.slug, a.parent_id,
			       a.total_word_count, a.regulation_count, a.checksum, a.updated_at,
			       (SELECT COUNT(*) FROM agency_titles WHERE agency_id = a.id) as title_count
			FROM agencies a
			ORDER BY a.total_word_count %s
		`, sortOrder)
	}

	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to get agencies: %w", err)
	}
	defer rows.Close()

	var result []AgencyWithDepth
	for rows.Next() {
		var a AgencyWithDepth
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
			&a.TitleCount,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan agency: %w", err)
		}
		result = append(result, a)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	// Calculate percentile-based density scores
	calculateDensityScores(result)

	return result, nil
}

// GetByID retrieves an agency by its ID
func (s *AgencyStore) GetByID(ctx context.Context, id int) (*model.Agency, error) {
	query := `
		SELECT id, agency_name, short_name, slug, parent_id, total_word_count,
		       regulation_count, checksum, updated_at
		FROM agencies
		WHERE id = $1
	`

	var a model.Agency
	err := s.db.QueryRowContext(ctx, query, id).Scan(
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
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get agency %d: %w", id, err)
	}

	return &a, nil
}

// GetChildren retrieves all child agencies for a parent
func (s *AgencyStore) GetChildren(ctx context.Context, parentID int) ([]model.Agency, error) {
	query := `
		SELECT id, agency_name, short_name, slug, parent_id, total_word_count,
		       regulation_count, checksum, updated_at
		FROM agencies
		WHERE parent_id = $1
		ORDER BY agency_name
	`

	rows, err := s.db.QueryContext(ctx, query, parentID)
	if err != nil {
		return nil, fmt.Errorf("failed to get children for agency %d: %w", parentID, err)
	}
	defer rows.Close()

	var children []model.Agency
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
			return nil, fmt.Errorf("failed to scan child agency: %w", err)
		}
		children = append(children, a)
	}

	return children, rows.Err()
}

// GetTitlesForAgency retrieves full title objects linked to an agency
func (s *AgencyStore) GetTitlesForAgency(ctx context.Context, agencyID int) ([]model.Title, error) {
	query := `
		SELECT t.id, t.title_number, t.title_name, t.word_count,
		       t.section_count, t.checksum, t.last_amended_date, t.fetched_at, t.created_at
		FROM titles t
		INNER JOIN agency_titles at ON t.title_number = at.title_number
		WHERE at.agency_id = $1
		ORDER BY t.title_number
	`

	rows, err := s.db.QueryContext(ctx, query, agencyID)
	if err != nil {
		return nil, fmt.Errorf("failed to get titles for agency %d: %w", agencyID, err)
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

// GetSnapshotsForAgency retrieves all snapshots for an agency
func (s *AgencyStore) GetSnapshotsForAgency(ctx context.Context, agencyID int) ([]model.AgencySnapshot, error) {
	query := `
		SELECT id, agency_id, agency_name, total_word_count, regulation_count,
		       checksum, snapshot_date, created_at
		FROM agency_snapshots
		WHERE agency_id = $1
		ORDER BY snapshot_date DESC
	`

	rows, err := s.db.QueryContext(ctx, query, agencyID)
	if err != nil {
		return nil, fmt.Errorf("failed to get snapshots for agency %d: %w", agencyID, err)
	}
	defer rows.Close()

	var snapshots []model.AgencySnapshot
	for rows.Next() {
		var snap model.AgencySnapshot
		err := rows.Scan(
			&snap.ID,
			&snap.AgencyID,
			&snap.AgencyName,
			&snap.TotalWordCount,
			&snap.RegulationCount,
			&snap.Checksum,
			&snap.SnapshotDate,
			&snap.CreatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan agency snapshot: %w", err)
		}
		snapshots = append(snapshots, snap)
	}

	return snapshots, rows.Err()
}

// CountAgencies returns the total number of agencies
func (s *AgencyStore) CountAgencies(ctx context.Context) (int, error) {
	var count int
	err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM agencies").Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to count agencies: %w", err)
	}
	return count, nil
}

// GetAgencySnapshotDates returns all unique snapshot dates for agencies
func (s *AgencyStore) GetAgencySnapshotDates(ctx context.Context) ([]time.Time, error) {
	query := `SELECT DISTINCT snapshot_date FROM agency_snapshots ORDER BY snapshot_date DESC`
	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to get agency snapshot dates: %w", err)
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

// GetTitleCountForAgency returns the number of titles linked to an agency
func (s *AgencyStore) GetTitleCountForAgency(ctx context.Context, agencyID int) (int, error) {
	var count int
	err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM agency_titles WHERE agency_id = $1", agencyID).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to count titles for agency %d: %w", agencyID, err)
	}
	return count, nil
}
