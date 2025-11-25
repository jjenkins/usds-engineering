package handlers

import (
	"context"
	"strconv"

	"github.com/a-h/templ"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/adaptor"
	"github.com/jjenkins/usds/internal/store"
	"github.com/jjenkins/usds/internal/templates"
)

func TitlesHandler(titleStore *store.TitleStore) fiber.Handler {
	return func(c *fiber.Ctx) error {
		ctx := context.Background()

		sortBy := c.Query("sort", "number")
		order := c.Query("order", "asc")

		titles, err := titleStore.GetAllSortedWithDensity(ctx, sortBy, order)
		if err != nil {
			return c.Status(fiber.StatusInternalServerError).SendString("Error loading titles")
		}

		// Check if this is an HTMX request for just the table body
		if c.Get("HX-Request") == "true" {
			page := templates.TitlesTableBody(titles, sortBy, order)
			handler := adaptor.HTTPHandler(templ.Handler(page))
			return handler(c)
		}

		page := templates.Titles(titles, sortBy, order)
		handler := adaptor.HTTPHandler(templ.Handler(page))

		return handler(c)
	}
}

func TitleDetailHandler(titleStore *store.TitleStore) fiber.Handler {
	return func(c *fiber.Ctx) error {
		ctx := context.Background()

		numberStr := c.Params("number")
		number, err := strconv.Atoi(numberStr)
		if err != nil {
			return c.Status(fiber.StatusBadRequest).SendString("Invalid title number")
		}

		title, err := titleStore.GetByNumber(ctx, number)
		if err != nil {
			return c.Status(fiber.StatusInternalServerError).SendString("Error loading title")
		}
		if title == nil {
			return c.Status(fiber.StatusNotFound).SendString("Title not found")
		}

		snapshots, err := titleStore.GetSnapshots(ctx, number)
		if err != nil {
			return c.Status(fiber.StatusInternalServerError).SendString("Error loading snapshots")
		}

		agencies, err := titleStore.GetAgenciesForTitle(ctx, number)
		if err != nil {
			return c.Status(fiber.StatusInternalServerError).SendString("Error loading agencies")
		}

		// Calculate density score
		densityScore, _ := titleStore.GetDensityScoreForTitle(ctx, title)

		page := templates.TitleDetail(title, snapshots, agencies, densityScore)
		handler := adaptor.HTTPHandler(templ.Handler(page))

		return handler(c)
	}
}
