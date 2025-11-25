package service

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/jjenkins/usds/internal/model"
)

const (
	baseURL        = "https://www.ecfr.gov/api/versioner/v1"
	adminBaseURL   = "https://www.ecfr.gov/api/admin/v1"
	defaultTimeout = 120 * time.Second // Increased for large historical titles
	maxRetries     = 3
	initialBackoff = 2 * time.Second // Longer initial backoff for 504s
	requestDelay   = 1 * time.Second
)

// ECFRClient handles communication with the eCFR API
type ECFRClient struct {
	client *http.Client
}

// NewECFRClient creates a new eCFR API client
func NewECFRClient() *ECFRClient {
	return &ECFRClient{
		client: &http.Client{
			Timeout: defaultTimeout,
		},
	}
}

// titlesResponse represents the API response for /titles.json
type titlesResponse struct {
	Titles []struct {
		Number          int    `json:"number"`
		Name            string `json:"name"`
		LatestAmendedOn string `json:"latest_amended_on"`
		LatestIssueDate string `json:"latest_issue_date"`
		Reserved        bool   `json:"reserved"`
	} `json:"titles"`
}

// agenciesResponse represents the API response for /agencies.json
type agenciesResponse struct {
	Agencies []agencyJSON `json:"agencies"`
}

// agencyJSON represents an agency in the API response (recursive for children)
type agencyJSON struct {
	Name          string       `json:"name"`
	ShortName     string       `json:"short_name"`
	Slug          string       `json:"slug"`
	Children      []agencyJSON `json:"children"`
	CFRReferences []struct {
		Title   int    `json:"title"`
		Chapter string `json:"chapter"`
	} `json:"cfr_references"`
}

// FetchTitles retrieves the list of all CFR titles
func (c *ECFRClient) FetchTitles(ctx context.Context) ([]model.TitleMeta, error) {
	url := fmt.Sprintf("%s/titles.json", baseURL)

	body, err := c.fetchWithRetry(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch titles: %w", err)
	}

	var resp titlesResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("failed to parse titles response: %w", err)
	}

	titles := make([]model.TitleMeta, len(resp.Titles))
	for i, t := range resp.Titles {
		titles[i] = model.TitleMeta{
			Number:          t.Number,
			Name:            t.Name,
			LatestAmendedOn: t.LatestAmendedOn,
			LatestIssueDate: t.LatestIssueDate,
			Reserved:        t.Reserved,
		}
	}

	return titles, nil
}

// FetchTitleContent retrieves the full XML content for a title
func (c *ECFRClient) FetchTitleContent(ctx context.Context, date string, titleNumber int) ([]byte, error) {
	url := fmt.Sprintf("%s/full/%s/title-%d.xml", baseURL, date, titleNumber)

	body, err := c.fetchWithRetry(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch title %d content: %w", titleNumber, err)
	}

	return body, nil
}

// FetchAgencies retrieves all agencies with their hierarchical structure
func (c *ECFRClient) FetchAgencies(ctx context.Context) ([]model.AgencyMeta, error) {
	url := fmt.Sprintf("%s/agencies.json", adminBaseURL)

	body, err := c.fetchWithRetry(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch agencies: %w", err)
	}

	var resp agenciesResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("failed to parse agencies response: %w", err)
	}

	agencies := make([]model.AgencyMeta, len(resp.Agencies))
	for i, a := range resp.Agencies {
		agencies[i] = convertAgencyJSON(a)
	}

	return agencies, nil
}

// convertAgencyJSON recursively converts API agency to model
func convertAgencyJSON(a agencyJSON) model.AgencyMeta {
	agency := model.AgencyMeta{
		Name:      a.Name,
		ShortName: a.ShortName,
		Slug:      a.Slug,
		Children:  make([]model.AgencyMeta, len(a.Children)),
	}

	for _, ref := range a.CFRReferences {
		agency.CFRReferences = append(agency.CFRReferences, model.CFRReference{
			Title:   ref.Title,
			Chapter: ref.Chapter,
		})
	}

	for i, child := range a.Children {
		agency.Children[i] = convertAgencyJSON(child)
	}

	return agency
}

// fetchWithRetry performs an HTTP GET with exponential backoff retry
func (c *ECFRClient) fetchWithRetry(ctx context.Context, url string) ([]byte, error) {
	var lastErr error
	backoff := initialBackoff

	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(backoff):
				backoff *= 2
			}
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create request: %w", err)
		}

		resp, err := c.client.Do(req)
		if err != nil {
			lastErr = err
			continue
		}

		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()

		if err != nil {
			lastErr = err
			continue
		}

		if resp.StatusCode == http.StatusTooManyRequests {
			lastErr = fmt.Errorf("rate limited (HTTP 429)")
			continue
		}

		if resp.StatusCode != http.StatusOK {
			lastErr = fmt.Errorf("unexpected status code: %d", resp.StatusCode)
			continue
		}

		return body, nil
	}

	return nil, fmt.Errorf("failed after %d attempts: %w", maxRetries, lastErr)
}

// Delay returns the configured delay between requests
func (c *ECFRClient) Delay() time.Duration {
	return requestDelay
}

// versionsResponse represents the API response for /versions/title-{n}.json
type versionsResponse struct {
	ContentVersions []struct {
		Date       string `json:"date"`
		Identifier string `json:"identifier"`
	} `json:"content_versions"`
}

// FetchTitleVersions retrieves all available issue dates for a title
func (c *ECFRClient) FetchTitleVersions(ctx context.Context, titleNumber int) ([]string, error) {
	url := fmt.Sprintf("%s/versions/title-%d.json", baseURL, titleNumber)

	body, err := c.fetchWithRetry(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch versions for title %d: %w", titleNumber, err)
	}

	var resp versionsResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("failed to parse versions response: %w", err)
	}

	dates := make([]string, len(resp.ContentVersions))
	for i, v := range resp.ContentVersions {
		dates[i] = v.Date
	}

	return dates, nil
}
