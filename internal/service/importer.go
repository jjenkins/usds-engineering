package service

import (
	"context"
	"crypto/md5"
	"database/sql"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"sort"
	"time"

	"github.com/jjenkins/usds/internal/model"
	"github.com/jjenkins/usds/internal/store"
)

// ImportStats tracks import statistics
type ImportStats struct {
	Total     int
	Imported  int
	Changed   int
	Unchanged int
	Skipped   int
	Failed    int
}

// Importer orchestrates the eCFR data import process
type Importer struct {
	client      *ECFRClient
	parser      *Parser
	titleStore  *store.TitleStore
	agencyStore *store.AgencyStore
	logger      *log.Logger
	errLogger   *log.Logger
}

// NewImporter creates a new Importer
func NewImporter(client *ECFRClient, parser *Parser, titleStore *store.TitleStore, agencyStore *store.AgencyStore) *Importer {
	return &Importer{
		client:      client,
		parser:      parser,
		titleStore:  titleStore,
		agencyStore: agencyStore,
		logger:      log.New(os.Stdout, "", log.LstdFlags),
		errLogger:   log.New(os.Stderr, "ERROR: ", log.LstdFlags),
	}
}

// Import fetches and stores all eCFR titles for the given date
func (i *Importer) Import(ctx context.Context, date string) (*ImportStats, error) {
	stats := &ImportStats{}

	// Fetch list of all titles
	i.logger.Println("Fetching titles list from eCFR API...")
	titles, err := i.client.FetchTitles(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch titles list: %w", err)
	}

	stats.Total = len(titles)
	i.logger.Printf("Found %d titles to process", stats.Total)

	// Parse the snapshot date
	snapshotDate, err := time.Parse("2006-01-02", date)
	if err != nil {
		return nil, fmt.Errorf("invalid date format: %w", err)
	}

	// Process each title
	for idx, titleMeta := range titles {
		select {
		case <-ctx.Done():
			return stats, ctx.Err()
		default:
		}

		progress := fmt.Sprintf("[%d/%d]", idx+1, stats.Total)

		// Skip reserved titles
		if titleMeta.Reserved {
			i.logger.Printf("%s Skipping Title %d: %s (reserved)", progress, titleMeta.Number, titleMeta.Name)
			stats.Skipped++
			continue
		}

		i.logger.Printf("%s Importing Title %d: %s...", progress, titleMeta.Number, titleMeta.Name)

		if err := i.importTitle(ctx, titleMeta, date, snapshotDate, stats); err != nil {
			i.errLogger.Printf("Failed to import Title %d: %v", titleMeta.Number, err)
			stats.Failed++
			continue
		}

		stats.Imported++

		// Rate limiting delay between requests
		if idx < len(titles)-1 {
			time.Sleep(i.client.Delay())
		}
	}

	return stats, nil
}

// ImportSingleTitle imports a specific title by number for the given date
func (i *Importer) ImportSingleTitle(ctx context.Context, titleNumber int, date string, snapshotDate time.Time) (*ImportStats, error) {
	stats := &ImportStats{Total: 1}

	// Fetch the titles list to get metadata for the requested title
	i.logger.Println("Fetching title metadata from eCFR API...")
	titles, err := i.client.FetchTitles(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch titles list: %w", err)
	}

	// Find the requested title
	var titleMeta *model.TitleMeta
	for _, t := range titles {
		if t.Number == titleNumber {
			titleMeta = &t
			break
		}
	}

	if titleMeta == nil {
		return nil, fmt.Errorf("title %d not found in eCFR API", titleNumber)
	}

	if titleMeta.Reserved {
		i.logger.Printf("Title %d is reserved, skipping", titleNumber)
		stats.Skipped++
		return stats, nil
	}

	// Import the title
	i.logger.Printf("Importing Title %d: %s", titleMeta.Number, titleMeta.Name)
	if err := i.importTitle(ctx, *titleMeta, date, snapshotDate, stats); err != nil {
		i.errLogger.Printf("Failed to import Title %d: %v", titleMeta.Number, err)
		stats.Failed++
		return stats, err
	}

	stats.Imported++
	return stats, nil
}

// importTitle imports a single title
func (i *Importer) importTitle(ctx context.Context, meta model.TitleMeta, date string, snapshotDate time.Time, stats *ImportStats) error {
	// Use the provided date for fetching historical content
	// The eCFR API returns the content as it existed on that date
	fetchDate := date
	if fetchDate == "" {
		// Fallback to latest issue date if no date specified (shouldn't happen)
		fetchDate = meta.LatestIssueDate
	}

	// Fetch XML content for the specified date
	content, err := i.client.FetchTitleContent(ctx, fetchDate, meta.Number)
	if err != nil {
		return fmt.Errorf("failed to fetch content: %w", err)
	}

	// Parse content for metrics
	parseResult, err := i.parser.Parse(content)
	if err != nil {
		return fmt.Errorf("failed to parse content: %w", err)
	}

	// Parse last amended date
	var lastAmendedDate sql.NullTime
	if meta.LatestAmendedOn != "" {
		t, err := time.Parse("2006-01-02", meta.LatestAmendedOn)
		if err == nil {
			lastAmendedDate = sql.NullTime{Time: t, Valid: true}
		}
	}

	// Build title model
	title := &model.Title{
		TitleNumber:     meta.Number,
		TitleName:       meta.Name,
		WordCount:       parseResult.WordCount,
		SectionCount:    parseResult.SectionCount,
		Checksum:        parseResult.Checksum,
		LastAmendedDate: lastAmendedDate,
		FetchedAt:       time.Now(),
	}

	// Save title and snapshot (only creates snapshot if changed)
	changed, err := i.titleStore.SaveTitleWithSnapshot(ctx, title, snapshotDate)
	if err != nil {
		return fmt.Errorf("failed to save title: %w", err)
	}

	// Track change statistics
	if changed {
		i.logger.Printf("  Title %d changed (snapshot created)", meta.Number)
		stats.Changed++
	} else {
		i.logger.Printf("  Title %d unchanged", meta.Number)
		stats.Unchanged++
	}

	return nil
}

// PrintSummary prints the import statistics
func (i *Importer) PrintSummary(stats *ImportStats) {
	i.logger.Println("")
	i.logger.Println("=== Import Summary ===")
	i.logger.Printf("Total titles:    %d", stats.Total)
	i.logger.Printf("Imported:        %d", stats.Imported)
	i.logger.Printf("Changed:         %d", stats.Changed)
	i.logger.Printf("Unchanged:       %d", stats.Unchanged)
	i.logger.Printf("Skipped:         %d (reserved)", stats.Skipped)
	i.logger.Printf("Failed:          %d", stats.Failed)

	successRate := float64(stats.Imported) / float64(stats.Total-stats.Skipped) * 100
	i.logger.Printf("Success rate:    %.1f%%", successRate)
}

// AgencyStats tracks agency import statistics
type AgencyStats struct {
	Total    int
	Imported int
	Failed   int
}

// ImportAgencies fetches and stores all agencies with hierarchy
func (i *Importer) ImportAgencies(ctx context.Context, snapshotDate time.Time) (*AgencyStats, error) {
	stats := &AgencyStats{}

	i.logger.Println("Fetching agencies from eCFR Admin API...")
	agencies, err := i.client.FetchAgencies(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch agencies: %w", err)
	}

	i.logger.Printf("Found %d top-level agencies", len(agencies))

	// Clear existing agency-title links for re-import
	if err := i.agencyStore.ClearAgencyTitles(ctx); err != nil {
		return nil, fmt.Errorf("failed to clear agency titles: %w", err)
	}

	// Pass 1: Insert all agencies with hierarchy (flattened but with parent_id)
	i.logger.Println("Pass 1: Inserting agencies...")
	slugToID := make(map[string]int)
	if err := i.insertAgenciesRecursive(ctx, agencies, sql.NullInt64{}, slugToID, stats); err != nil {
		return nil, fmt.Errorf("failed to insert agencies: %w", err)
	}

	// Pass 2: Link agencies to titles via cfr_references
	i.logger.Println("Pass 2: Linking agencies to titles...")
	if err := i.linkAgenciesToTitles(ctx, agencies, slugToID); err != nil {
		return nil, fmt.Errorf("failed to link agencies: %w", err)
	}

	// Pass 3: Calculate roll-up word counts (bottom-up)
	i.logger.Println("Pass 3: Calculating roll-up word counts...")
	if err := i.calculateRollupWordCounts(ctx, snapshotDate); err != nil {
		return nil, fmt.Errorf("failed to calculate word counts: %w", err)
	}

	return stats, nil
}

// insertAgenciesRecursive inserts agencies and their children, tracking slug->ID mapping
func (i *Importer) insertAgenciesRecursive(ctx context.Context, agencies []model.AgencyMeta, parentID sql.NullInt64, slugToID map[string]int, stats *AgencyStats) error {
	for _, meta := range agencies {
		stats.Total++

		agency := &model.Agency{
			AgencyName: meta.Name,
			ShortName:  sql.NullString{String: meta.ShortName, Valid: meta.ShortName != ""},
			Slug:       meta.Slug,
			ParentID:   parentID,
		}

		if err := i.agencyStore.UpsertAgency(ctx, agency); err != nil {
			i.errLogger.Printf("Failed to insert agency %s: %v", meta.Slug, err)
			stats.Failed++
			continue
		}

		slugToID[meta.Slug] = agency.ID
		stats.Imported++

		// Recursively insert children with this agency as parent
		if len(meta.Children) > 0 {
			childParentID := sql.NullInt64{Int64: int64(agency.ID), Valid: true}
			if err := i.insertAgenciesRecursive(ctx, meta.Children, childParentID, slugToID, stats); err != nil {
				return err
			}
		}
	}

	return nil
}

// linkAgenciesToTitles creates agency-title links based on cfr_references
func (i *Importer) linkAgenciesToTitles(ctx context.Context, agencies []model.AgencyMeta, slugToID map[string]int) error {
	for _, meta := range agencies {
		agencyID, ok := slugToID[meta.Slug]
		if !ok {
			continue
		}

		for _, ref := range meta.CFRReferences {
			if err := i.agencyStore.LinkAgencyTitle(ctx, agencyID, ref.Title); err != nil {
				i.errLogger.Printf("Failed to link agency %s to title %d: %v", meta.Slug, ref.Title, err)
			}
		}

		// Recursively link children
		if len(meta.Children) > 0 {
			if err := i.linkAgenciesToTitles(ctx, meta.Children, slugToID); err != nil {
				return err
			}
		}
	}

	return nil
}

// calculateRollupWordCounts calculates word counts for all agencies
// Parents include all descendants' word counts (de-duplicated by title)
func (i *Importer) calculateRollupWordCounts(ctx context.Context, snapshotDate time.Time) error {
	agencies, err := i.agencyStore.GetAll(ctx)
	if err != nil {
		return fmt.Errorf("failed to get agencies: %w", err)
	}

	// Build ID -> Agency map and parent -> children map
	agencyMap := make(map[int]*model.Agency)
	childrenMap := make(map[int][]int)
	var rootIDs []int

	for idx := range agencies {
		a := &agencies[idx]
		agencyMap[a.ID] = a
		if a.ParentID.Valid {
			parentID := int(a.ParentID.Int64)
			childrenMap[parentID] = append(childrenMap[parentID], a.ID)
		} else {
			rootIDs = append(rootIDs, a.ID)
		}
	}

	// Calculate word counts recursively for each root
	for _, rootID := range rootIDs {
		if _, err := i.calculateAgencyWordCount(ctx, rootID, agencyMap, childrenMap, snapshotDate); err != nil {
			return err
		}
	}

	return nil
}

// calculateAgencyWordCount recursively calculates word count for an agency
// Returns the set of title numbers used (for de-duplication)
func (i *Importer) calculateAgencyWordCount(ctx context.Context, agencyID int, agencyMap map[int]*model.Agency, childrenMap map[int][]int, snapshotDate time.Time) (map[int]bool, error) {
	agency := agencyMap[agencyID]
	titleSet := make(map[int]bool)

	// Get directly linked titles
	directTitles, err := i.agencyStore.GetAgencyTitles(ctx, agencyID)
	if err != nil {
		return nil, fmt.Errorf("failed to get titles for agency %d: %w", agencyID, err)
	}
	for _, t := range directTitles {
		titleSet[t] = true
	}

	// Recursively get titles from children
	for _, childID := range childrenMap[agencyID] {
		childTitles, err := i.calculateAgencyWordCount(ctx, childID, agencyMap, childrenMap, snapshotDate)
		if err != nil {
			return nil, err
		}
		for t := range childTitles {
			titleSet[t] = true
		}
	}

	// Sort title numbers for consistent checksum
	var titleNums []int
	for titleNum := range titleSet {
		titleNums = append(titleNums, titleNum)
	}
	sort.Ints(titleNums)

	// Sum word counts from all unique titles and build checksum input
	totalWordCount := 0
	checksumInput := ""
	for _, titleNum := range titleNums {
		wordCount, err := i.agencyStore.GetTitleWordCount(ctx, titleNum)
		if err != nil {
			i.errLogger.Printf("Failed to get word count for title %d: %v", titleNum, err)
			continue
		}
		totalWordCount += wordCount
		checksumInput += fmt.Sprintf("%d:%d;", titleNum, wordCount)
	}

	// Generate MD5 checksum for change detection
	hash := md5.Sum([]byte(checksumInput))
	checksum := hex.EncodeToString(hash[:])

	// Update agency with calculated counts
	if err := i.agencyStore.UpdateWordCount(ctx, agencyID, totalWordCount, len(titleSet), checksum); err != nil {
		return nil, fmt.Errorf("failed to update word count for agency %d: %w", agencyID, err)
	}

	// Create snapshot only if changed
	snapshot := &model.AgencySnapshot{
		AgencyID:        agencyID,
		AgencyName:      agency.AgencyName,
		TotalWordCount:  totalWordCount,
		RegulationCount: len(titleSet),
		Checksum:        checksum,
		SnapshotDate:    snapshotDate,
	}
	snapshotCreated, err := i.agencyStore.InsertSnapshotIfChanged(ctx, snapshot, titleNums)
	if err != nil {
		i.errLogger.Printf("Failed to insert snapshot for agency %d: %v", agencyID, err)
	}

	if snapshotCreated {
		i.logger.Printf("  Agency %s: %d words, %d titles (snapshot created)", agency.AgencyName, totalWordCount, len(titleSet))
	} else {
		i.logger.Printf("  Agency %s: %d words, %d titles (unchanged)", agency.AgencyName, totalWordCount, len(titleSet))
	}

	return titleSet, nil
}

// PrintAgencySummary prints agency import statistics
func (i *Importer) PrintAgencySummary(stats *AgencyStats) {
	i.logger.Println("")
	i.logger.Println("=== Agency Import Summary ===")
	i.logger.Printf("Total agencies:  %d", stats.Total)
	i.logger.Printf("Imported:        %d", stats.Imported)
	i.logger.Printf("Failed:          %d", stats.Failed)
}

// HistoricalStats tracks historical import statistics
type HistoricalStats struct {
	TitlesProcessed   int
	VersionsProcessed int
	SnapshotsCreated  int
	Failed            int
}

// ImportAllHistory fetches all historical versions for all titles
func (i *Importer) ImportAllHistory(ctx context.Context) (*HistoricalStats, error) {
	stats := &HistoricalStats{}

	// Fetch list of all titles
	i.logger.Println("Fetching titles list from eCFR API...")
	titles, err := i.client.FetchTitles(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch titles list: %w", err)
	}

	i.logger.Printf("Found %d titles", len(titles))

	// Process each title
	for titleIdx, titleMeta := range titles {
		select {
		case <-ctx.Done():
			return stats, ctx.Err()
		default:
		}

		// Skip reserved titles
		if titleMeta.Reserved {
			i.logger.Printf("[%d/%d] Skipping Title %d: %s (reserved)", titleIdx+1, len(titles), titleMeta.Number, titleMeta.Name)
			continue
		}

		i.logger.Printf("[%d/%d] Fetching versions for Title %d: %s...", titleIdx+1, len(titles), titleMeta.Number, titleMeta.Name)

		// Fetch all versions for this title
		versions, err := i.client.FetchTitleVersions(ctx, titleMeta.Number)
		if err != nil {
			i.errLogger.Printf("Failed to fetch versions for Title %d: %v", titleMeta.Number, err)
			stats.Failed++
			continue
		}

		i.logger.Printf("  Found %d versions for Title %d", len(versions), titleMeta.Number)
		stats.TitlesProcessed++

		// Import each version
		for versionIdx, versionDate := range versions {
			select {
			case <-ctx.Done():
				return stats, ctx.Err()
			default:
			}

			// Parse the version date for snapshot
			snapshotDate, err := time.Parse("2006-01-02", versionDate)
			if err != nil {
				i.errLogger.Printf("Invalid date format %s: %v", versionDate, err)
				stats.Failed++
				continue
			}

			i.logger.Printf("  [%d/%d] Importing version %s...", versionIdx+1, len(versions), versionDate)

			// Fetch XML content for this version
			content, err := i.client.FetchTitleContent(ctx, versionDate, titleMeta.Number)
			if err != nil {
				i.errLogger.Printf("Failed to fetch content for Title %d date %s: %v", titleMeta.Number, versionDate, err)
				stats.Failed++
				time.Sleep(i.client.Delay())
				continue
			}

			// Parse content for metrics
			parseResult, err := i.parser.Parse(content)
			if err != nil {
				i.errLogger.Printf("Failed to parse content for Title %d date %s: %v", titleMeta.Number, versionDate, err)
				stats.Failed++
				continue
			}

			// Parse last amended date from meta
			var lastAmendedDate sql.NullTime
			if titleMeta.LatestAmendedOn != "" {
				t, err := time.Parse("2006-01-02", titleMeta.LatestAmendedOn)
				if err == nil {
					lastAmendedDate = sql.NullTime{Time: t, Valid: true}
				}
			}

			// Build title model
			title := &model.Title{
				TitleNumber:     titleMeta.Number,
				TitleName:       titleMeta.Name,
				WordCount:       parseResult.WordCount,
				SectionCount:    parseResult.SectionCount,
				Checksum:        parseResult.Checksum,
				LastAmendedDate: lastAmendedDate,
				FetchedAt:       time.Now(),
			}

			// Save title and snapshot
			changed, err := i.titleStore.SaveTitleWithSnapshot(ctx, title, snapshotDate)
			if err != nil {
				i.errLogger.Printf("Failed to save Title %d date %s: %v", titleMeta.Number, versionDate, err)
				stats.Failed++
				continue
			}

			stats.VersionsProcessed++
			if changed {
				stats.SnapshotsCreated++
				i.logger.Printf("    Snapshot created: %d words, %d sections", parseResult.WordCount, parseResult.SectionCount)
			} else {
				i.logger.Printf("    Unchanged (duplicate checksum)")
			}

			// Rate limiting
			time.Sleep(i.client.Delay())
		}
	}

	return stats, nil
}

// PrintHistoricalSummary prints historical import statistics
func (i *Importer) PrintHistoricalSummary(stats *HistoricalStats) {
	i.logger.Println("")
	i.logger.Println("=== Historical Import Summary ===")
	i.logger.Printf("Titles processed:   %d", stats.TitlesProcessed)
	i.logger.Printf("Versions processed: %d", stats.VersionsProcessed)
	i.logger.Printf("Snapshots created:  %d", stats.SnapshotsCreated)
	i.logger.Printf("Failed:             %d", stats.Failed)
}
