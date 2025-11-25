-- eCFR Analyzer Database Schema

-- Titles: Current state of each CFR title (50 total)
CREATE TABLE IF NOT EXISTS titles (
    id SERIAL PRIMARY KEY,
    title_number INTEGER UNIQUE NOT NULL,
    title_name TEXT NOT NULL,
    full_content TEXT,
    word_count INTEGER DEFAULT 0,
    section_count INTEGER DEFAULT 0,
    checksum TEXT,
    last_amended_date DATE,
    fetched_at TIMESTAMP DEFAULT NOW(),
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_titles_number ON titles(title_number);
CREATE INDEX IF NOT EXISTS idx_titles_checksum ON titles(checksum);

-- Title Snapshots: Historical versions for change tracking
CREATE TABLE IF NOT EXISTS title_snapshots (
    id SERIAL PRIMARY KEY,
    title_number INTEGER NOT NULL,
    title_name TEXT NOT NULL,
    full_content TEXT,
    word_count INTEGER DEFAULT 0,
    section_count INTEGER DEFAULT 0,
    checksum TEXT NOT NULL,
    last_amended_date DATE,
    snapshot_date DATE NOT NULL,
    created_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(title_number, snapshot_date)
);

CREATE INDEX IF NOT EXISTS idx_snapshots_title ON title_snapshots(title_number);
CREATE INDEX IF NOT EXISTS idx_snapshots_date ON title_snapshots(snapshot_date);
CREATE INDEX IF NOT EXISTS idx_snapshots_checksum ON title_snapshots(checksum);

-- Agencies: Federal agencies that issue regulations
CREATE TABLE IF NOT EXISTS agencies (
    id SERIAL PRIMARY KEY,
    agency_name TEXT UNIQUE NOT NULL,
    slug TEXT UNIQUE NOT NULL,
    total_word_count INTEGER DEFAULT 0,
    regulation_count INTEGER DEFAULT 0,
    checksum TEXT,
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_agencies_slug ON agencies(slug);

-- Agency-Title Junction: Many-to-many relationship between agencies and titles
CREATE TABLE IF NOT EXISTS agency_titles (
    agency_id INTEGER NOT NULL REFERENCES agencies(id) ON DELETE CASCADE,
    title_number INTEGER NOT NULL REFERENCES titles(title_number) ON DELETE CASCADE,
    PRIMARY KEY (agency_id, title_number)
);

CREATE INDEX IF NOT EXISTS idx_agency_titles_agency ON agency_titles(agency_id);
CREATE INDEX IF NOT EXISTS idx_agency_titles_title ON agency_titles(title_number);

-- Agency Snapshots: Historical agency metrics
CREATE TABLE IF NOT EXISTS agency_snapshots (
    id SERIAL PRIMARY KEY,
    agency_id INTEGER NOT NULL REFERENCES agencies(id) ON DELETE CASCADE,
    agency_name TEXT NOT NULL,
    total_word_count INTEGER DEFAULT 0,
    regulation_count INTEGER DEFAULT 0,
    checksum TEXT NOT NULL,
    snapshot_date DATE NOT NULL,
    created_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(agency_id, snapshot_date)
);

CREATE INDEX IF NOT EXISTS idx_agency_snapshots_agency ON agency_snapshots(agency_id);
CREATE INDEX IF NOT EXISTS idx_agency_snapshots_date ON agency_snapshots(snapshot_date);

-- Agency Snapshot Titles: Historical junction for agency-title relationships
CREATE TABLE IF NOT EXISTS agency_snapshot_titles (
    agency_snapshot_id INTEGER NOT NULL REFERENCES agency_snapshots(id) ON DELETE CASCADE,
    title_number INTEGER NOT NULL,
    PRIMARY KEY (agency_snapshot_id, title_number)
);

CREATE INDEX IF NOT EXISTS idx_agency_snapshot_titles_snapshot ON agency_snapshot_titles(agency_snapshot_id);

-- Metrics: Calculated system-wide metrics
CREATE TABLE IF NOT EXISTS metrics (
    id SERIAL PRIMARY KEY,
    metric_name TEXT NOT NULL,
    metric_value TEXT NOT NULL,
    metric_type TEXT,
    entity_id INTEGER,
    calculated_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_metrics_name ON metrics(metric_name);
CREATE INDEX IF NOT EXISTS idx_metrics_entity ON metrics(entity_id, metric_type);
