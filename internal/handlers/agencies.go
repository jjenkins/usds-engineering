package handlers

import (
	"context"

	"github.com/a-h/templ"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/adaptor"
	"github.com/jjenkins/usds/internal/model"
	"github.com/jjenkins/usds/internal/store"
	"github.com/jjenkins/usds/internal/templates"
)

func AgenciesHandler(agencyStore *store.AgencyStore) fiber.Handler {
	return func(c *fiber.Ctx) error {
		ctx := context.Background()

		sortBy := c.Query("sort", "name")
		order := c.Query("order", "asc")

		agencies, err := agencyStore.GetAllSorted(ctx, sortBy, order)
		if err != nil {
			return c.Status(fiber.StatusInternalServerError).SendString("Error loading agencies")
		}

		// Check if this is an HTMX request for just the table body
		if c.Get("HX-Request") == "true" {
			page := templates.AgenciesTableBody(agencies, sortBy, order)
			handler := adaptor.HTTPHandler(templ.Handler(page))
			return handler(c)
		}

		page := templates.Agencies(agencies, sortBy, order)
		handler := adaptor.HTTPHandler(templ.Handler(page))

		return handler(c)
	}
}

func AgencyDetailHandler(agencyStore *store.AgencyStore) fiber.Handler {
	return func(c *fiber.Ctx) error {
		ctx := context.Background()

		slug := c.Params("slug")

		agency, err := agencyStore.GetBySlug(ctx, slug)
		if err != nil {
			return c.Status(fiber.StatusInternalServerError).SendString("Error loading agency")
		}
		if agency == nil {
			return c.Status(fiber.StatusNotFound).SendString("Agency not found")
		}

		// Get parent agency if exists
		var parent *model.Agency
		if agency.ParentID.Valid {
			parent, _ = agencyStore.GetByID(ctx, int(agency.ParentID.Int64))
		}

		// Get child agencies
		children, err := agencyStore.GetChildren(ctx, agency.ID)
		if err != nil {
			return c.Status(fiber.StatusInternalServerError).SendString("Error loading child agencies")
		}

		// Get linked titles
		titles, err := agencyStore.GetTitlesForAgency(ctx, agency.ID)
		if err != nil {
			return c.Status(fiber.StatusInternalServerError).SendString("Error loading titles")
		}

		// Get snapshots
		snapshots, err := agencyStore.GetSnapshotsForAgency(ctx, agency.ID)
		if err != nil {
			return c.Status(fiber.StatusInternalServerError).SendString("Error loading snapshots")
		}

		// Calculate density score
		densityScore, _ := agencyStore.GetDensityScoreForAgency(ctx, agency)

		page := templates.AgencyDetail(agency, parent, children, titles, snapshots, densityScore)
		handler := adaptor.HTTPHandler(templ.Handler(page))

		return handler(c)
	}
}
