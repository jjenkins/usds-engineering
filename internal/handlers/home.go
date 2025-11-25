package handlers

import (
	"context"
	"log"

	"github.com/a-h/templ"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/adaptor"
	"github.com/jjenkins/usds/internal/store"
	"github.com/jjenkins/usds/internal/templates"
)

func HomeHandler(titleStore *store.TitleStore, agencyStore *store.AgencyStore) fiber.Handler {
	return func(c *fiber.Ctx) error {
		ctx := context.Background()

		metrics := templates.HomeMetrics{}

		// Try to load metrics from database
		totalTitles, err := titleStore.CountTitles(ctx)
		if err != nil {
			log.Printf("Error counting titles: %v", err)
		} else {
			metrics.TotalTitles = totalTitles
			metrics.HasData = totalTitles > 0
		}

		if metrics.HasData {
			totalWords, err := titleStore.GetTotalWordCount(ctx)
			if err != nil {
				log.Printf("Error getting total word count: %v", err)
			} else {
				metrics.TotalWords = totalWords
			}

			avgDensity, err := titleStore.GetAverageDensity(ctx)
			if err != nil {
				log.Printf("Error getting average density: %v", err)
			} else {
				metrics.AverageDensity = avgDensity
			}

			totalAgencies, err := agencyStore.CountAgencies(ctx)
			if err != nil {
				log.Printf("Error counting agencies: %v", err)
			} else {
				metrics.TotalAgencies = totalAgencies
			}
		}

		page := templates.Home(metrics)
		handler := adaptor.HTTPHandler(templ.Handler(page))

		return handler(c)
	}
}
