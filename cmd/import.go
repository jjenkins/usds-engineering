package cmd

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jjenkins/usds/internal/service"
	"github.com/jjenkins/usds/internal/store"
	"github.com/spf13/cobra"
)

var importDate string
var importAllHistory bool
var importTitleNumber int

var importCmd = &cobra.Command{
	Use:   "import",
	Short: "Import eCFR data from the federal regulations API",
	Long: `Import downloads and stores Federal Regulation data from the eCFR API.

The import command fetches all CFR titles (~50), parses the XML content,
calculates metrics (word count, section count, checksum), and stores
the data in PostgreSQL with historical snapshot tracking.

Examples:
  # Import data for today's date
  ./usds import

  # Import data for a specific date
  ./usds import --date 2025-01-15

  # Import a single title
  ./usds import --title 40 --date 2020-01-01

  # Import all historical versions (WARNING: this takes a long time!)
  ./usds import --all-history`,
	Run: runImport,
}

func init() {
	rootCmd.AddCommand(importCmd)

	today := time.Now().Format("2006-01-02")
	importCmd.Flags().StringVarP(&importDate, "date", "d", today, "Date to import data for (YYYY-MM-DD)")
	importCmd.Flags().IntVarP(&importTitleNumber, "title", "t", 0, "Import only a specific title number (1-50)")
	importCmd.Flags().BoolVar(&importAllHistory, "all-history", false, "Import all historical versions for all titles")
}

func runImport(cmd *cobra.Command, args []string) {
	// Get database URL from environment
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		log.Fatal("DATABASE_URL environment variable is required")
	}

	// Set up context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle interrupt signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		log.Println("\nReceived interrupt signal, shutting down...")
		cancel()
	}()

	// Connect to database
	log.Println("Connecting to database...")
	db, err := store.NewDB(dbURL)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()

	// Create dependencies
	client := service.NewECFRClient()
	parser := service.NewParser()
	titleStore := store.NewTitleStore(db)
	agencyStore := store.NewAgencyStore(db)
	importer := service.NewImporter(client, parser, titleStore, agencyStore)

	// Handle --all-history flag
	if importAllHistory {
		log.Println("Starting historical import for ALL versions...")
		log.Println("WARNING: This will take a very long time (potentially hours)")
		log.Println("")

		histStats, err := importer.ImportAllHistory(ctx)
		if err != nil {
			if ctx.Err() != nil {
				log.Println("Import cancelled")
				importer.PrintHistoricalSummary(histStats)
				os.Exit(1)
			}
			log.Fatalf("Historical import failed: %v", err)
		}
		importer.PrintHistoricalSummary(histStats)

		if histStats.Failed > 0 {
			os.Exit(1)
		}
		return
	}

	// Parse snapshot date
	snapshotDate, err := time.Parse("2006-01-02", importDate)
	if err != nil {
		log.Fatalf("Invalid date format: %v", err)
	}

	// Handle single title import
	if importTitleNumber > 0 {
		log.Printf("Starting import for title %d on date: %s", importTitleNumber, importDate)
		stats, err := importer.ImportSingleTitle(ctx, importTitleNumber, importDate, snapshotDate)
		if err != nil {
			if ctx.Err() != nil {
				log.Println("Import cancelled")
				os.Exit(1)
			}
			log.Fatalf("Import failed: %v", err)
		}
		importer.PrintSummary(stats)
		if stats.Failed > 0 {
			os.Exit(1)
		}
		return
	}

	// Run title import
	log.Printf("Starting import for date: %s", importDate)
	stats, err := importer.Import(ctx, importDate)
	if err != nil {
		if ctx.Err() != nil {
			log.Println("Import cancelled")
			os.Exit(1)
		}
		log.Fatalf("Import failed: %v", err)
	}
	importer.PrintSummary(stats)

	// Run agency import
	log.Println("\nStarting agency import...")
	agencyStats, err := importer.ImportAgencies(ctx, snapshotDate)
	if err != nil {
		if ctx.Err() != nil {
			log.Println("Import cancelled")
			os.Exit(1)
		}
		log.Fatalf("Agency import failed: %v", err)
	}
	importer.PrintAgencySummary(agencyStats)

	// Calculate and store system metrics
	log.Println("\nCalculating system metrics...")
	metricsService := service.NewMetricsService(db)
	systemMetrics, err := metricsService.CalculateAndStore(ctx)
	if err != nil {
		log.Printf("Warning: Failed to calculate metrics: %v", err)
	} else {
		log.Println("")
		log.Println("=== System Metrics ===")
		log.Printf("Total titles:     %d", systemMetrics.TotalTitles)
		log.Printf("Total words:      %d", systemMetrics.TotalWords)
		log.Printf("Total sections:   %d", systemMetrics.TotalSections)
		log.Printf("Total agencies:   %d", systemMetrics.TotalAgencies)
		log.Printf("Average density:  %.2f words/section", systemMetrics.AverageDensity)
		log.Printf("Largest title:    %s (%d words)", systemMetrics.LargestTitle, systemMetrics.LargestTitleWords)
		log.Printf("Top agency:       %s (%d words)", systemMetrics.TopAgency, systemMetrics.TopAgencyWords)
	}

	// Exit with error code if there were failures
	if stats.Failed > 0 || agencyStats.Failed > 0 {
		os.Exit(1)
	}
}
