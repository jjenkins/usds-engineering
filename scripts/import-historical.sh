#!/bin/sh
# Import historical eCFR data for quarterly snapshots
# Usage: ./scripts/import-historical.sh
# Or inside container: docker-compose exec app ./scripts/import-historical.sh

set -e

# Quarterly dates from 2017-2024 (32 dates total)
DATES="2017-01-01 2017-04-01 2017-07-01 2017-10-01 \
2018-01-01 2018-04-01 2018-07-01 2018-10-01 \
2019-01-01 2019-04-01 2019-07-01 2019-10-01 \
2020-01-01 2020-04-01 2020-07-01 2020-10-01 \
2021-01-01 2021-04-01 2021-07-01 2021-10-01 \
2022-01-01 2022-04-01 2022-07-01 2022-10-01 \
2023-01-01 2023-04-01 2023-07-01 2023-10-01 \
2024-01-01 2024-04-01 2024-07-01 2024-10-01"

echo "Starting historical import for 32 quarterly dates..."
echo ""

for date in $DATES; do
  echo "========================================"
  echo "Importing data for: $date"
  echo "Started at: $(date)"
  echo "========================================"

  go run main.go import --date "$date"

  echo ""
  echo "Completed $date at $(date)"
  echo ""

  # Brief pause between imports to be nice to the API
  sleep 5
done

echo "========================================"
echo "All historical imports complete!"
echo "Finished at: $(date)"
echo "========================================"
