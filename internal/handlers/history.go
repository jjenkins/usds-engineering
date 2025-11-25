package handlers

import (
	"context"
	"time"

	"github.com/a-h/templ"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/adaptor"
	"github.com/jjenkins/usds/internal/store"
	"github.com/jjenkins/usds/internal/templates"
)

type SnapshotSummary struct {
	Date        time.Time
	TitleCount  int
	TotalWords  int
	AgencyCount int
}

func HistoryHandler(titleStore *store.TitleStore, agencyStore *store.AgencyStore) fiber.Handler {
	return func(c *fiber.Ctx) error {
		ctx := context.Background()

		// Get unique snapshot dates from titles
		titleDates, err := titleStore.GetSnapshotDates(ctx)
		if err != nil {
			return c.Status(fiber.StatusInternalServerError).SendString("Error loading title snapshots")
		}

		// Get unique snapshot dates from agencies
		agencyDates, err := agencyStore.GetAgencySnapshotDates(ctx)
		if err != nil {
			return c.Status(fiber.StatusInternalServerError).SendString("Error loading agency snapshots")
		}

		// Merge and dedupe dates
		dateMap := make(map[string]time.Time)
		for _, d := range titleDates {
			key := d.Format("2006-01-02")
			dateMap[key] = d
		}
		for _, d := range agencyDates {
			key := d.Format("2006-01-02")
			if _, exists := dateMap[key]; !exists {
				dateMap[key] = d
			}
		}

		// Convert to sorted slice
		var dates []time.Time
		for _, d := range dateMap {
			dates = append(dates, d)
		}
		// Sort descending
		for i := 0; i < len(dates)-1; i++ {
			for j := i + 1; j < len(dates); j++ {
				if dates[j].After(dates[i]) {
					dates[i], dates[j] = dates[j], dates[i]
				}
			}
		}

		// Get current totals
		totalTitles, _ := titleStore.CountTitles(ctx)
		totalWords, _ := titleStore.GetTotalWordCount(ctx)
		totalAgencies, _ := agencyStore.CountAgencies(ctx)

		page := templates.History(dates, totalTitles, totalWords, totalAgencies)
		handler := adaptor.HTTPHandler(templ.Handler(page))

		return handler(c)
	}
}
