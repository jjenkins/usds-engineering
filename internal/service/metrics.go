package service

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// MetricsService calculates and stores system-wide metrics
type MetricsService struct {
	db *sql.DB
}

// NewMetricsService creates a new MetricsService
func NewMetricsService(db *sql.DB) *MetricsService {
	return &MetricsService{db: db}
}

// SystemMetrics represents calculated system-wide metrics
type SystemMetrics struct {
	TotalTitles       int
	TotalWords        int
	TotalSections     int
	TotalAgencies     int
	AverageDensity    float64
	LargestTitle      string
	LargestTitleWords int
	TopAgency         string
	TopAgencyWords    int
}

// CalculateAndStore calculates system metrics and stores them
func (m *MetricsService) CalculateAndStore(ctx context.Context) (*SystemMetrics, error) {
	metrics := &SystemMetrics{}

	// Calculate title metrics
	titleQuery := `
		SELECT
			COUNT(*) as total_titles,
			COALESCE(SUM(word_count), 0) as total_words,
			COALESCE(SUM(section_count), 0) as total_sections
		FROM titles
	`
	err := m.db.QueryRowContext(ctx, titleQuery).Scan(
		&metrics.TotalTitles,
		&metrics.TotalWords,
		&metrics.TotalSections,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to calculate title metrics: %w", err)
	}

	// Calculate average density
	if metrics.TotalSections > 0 {
		metrics.AverageDensity = float64(metrics.TotalWords) / float64(metrics.TotalSections)
	}

	// Find largest title
	largestQuery := `
		SELECT title_name, word_count
		FROM titles
		ORDER BY word_count DESC
		LIMIT 1
	`
	err = m.db.QueryRowContext(ctx, largestQuery).Scan(
		&metrics.LargestTitle,
		&metrics.LargestTitleWords,
	)
	if err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("failed to find largest title: %w", err)
	}

	// Count agencies
	agencyCountQuery := `SELECT COUNT(*) FROM agencies`
	err = m.db.QueryRowContext(ctx, agencyCountQuery).Scan(&metrics.TotalAgencies)
	if err != nil {
		return nil, fmt.Errorf("failed to count agencies: %w", err)
	}

	// Find top agency by word count (only root-level agencies for meaningful comparison)
	topAgencyQuery := `
		SELECT agency_name, total_word_count
		FROM agencies
		WHERE parent_id IS NULL
		ORDER BY total_word_count DESC
		LIMIT 1
	`
	err = m.db.QueryRowContext(ctx, topAgencyQuery).Scan(
		&metrics.TopAgency,
		&metrics.TopAgencyWords,
	)
	if err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("failed to find top agency: %w", err)
	}

	// Store metrics
	if err := m.storeMetric(ctx, "total_titles", fmt.Sprintf("%d", metrics.TotalTitles)); err != nil {
		return nil, err
	}
	if err := m.storeMetric(ctx, "total_words", fmt.Sprintf("%d", metrics.TotalWords)); err != nil {
		return nil, err
	}
	if err := m.storeMetric(ctx, "total_sections", fmt.Sprintf("%d", metrics.TotalSections)); err != nil {
		return nil, err
	}
	if err := m.storeMetric(ctx, "total_agencies", fmt.Sprintf("%d", metrics.TotalAgencies)); err != nil {
		return nil, err
	}
	if err := m.storeMetric(ctx, "average_density", fmt.Sprintf("%.2f", metrics.AverageDensity)); err != nil {
		return nil, err
	}
	if err := m.storeMetric(ctx, "largest_title", metrics.LargestTitle); err != nil {
		return nil, err
	}
	if err := m.storeMetric(ctx, "top_agency", metrics.TopAgency); err != nil {
		return nil, err
	}

	return metrics, nil
}

// storeMetric stores a single metric value
func (m *MetricsService) storeMetric(ctx context.Context, name, value string) error {
	query := `
		INSERT INTO metrics (metric_name, metric_value, calculated_at)
		VALUES ($1, $2, $3)
	`

	_, err := m.db.ExecContext(ctx, query, name, value, time.Now())
	if err != nil {
		return fmt.Errorf("failed to store metric %s: %w", name, err)
	}

	return nil
}

// GetLatestMetrics retrieves the most recent system metrics
func (m *MetricsService) GetLatestMetrics(ctx context.Context) (map[string]string, error) {
	query := `
		SELECT DISTINCT ON (metric_name) metric_name, metric_value
		FROM metrics
		ORDER BY metric_name, calculated_at DESC
	`

	rows, err := m.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to get metrics: %w", err)
	}
	defer rows.Close()

	metrics := make(map[string]string)
	for rows.Next() {
		var name, value string
		if err := rows.Scan(&name, &value); err != nil {
			return nil, fmt.Errorf("failed to scan metric: %w", err)
		}
		metrics[name] = value
	}

	return metrics, rows.Err()
}
