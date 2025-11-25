package service

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"encoding/xml"
	"strings"
)

// ParseResult contains the metrics extracted from XML content
type ParseResult struct {
	WordCount    int
	SectionCount int
	Checksum     string
}

// Parser handles XML content parsing
type Parser struct{}

// NewParser creates a new Parser
func NewParser() *Parser {
	return &Parser{}
}

// Parse extracts metrics from XML content
func (p *Parser) Parse(content []byte) (*ParseResult, error) {
	result := &ParseResult{
		Checksum: p.calculateChecksum(content),
	}

	decoder := xml.NewDecoder(bytes.NewReader(content))

	var textBuilder strings.Builder
	var inTextElement bool

	for {
		token, err := decoder.Token()
		if err != nil {
			break // End of document or error
		}

		switch t := token.(type) {
		case xml.StartElement:
			// Count sections: DIV8 with TYPE="SECTION"
			if t.Name.Local == "DIV8" {
				for _, attr := range t.Attr {
					if attr.Name.Local == "TYPE" && attr.Value == "SECTION" {
						result.SectionCount++
						break
					}
				}
			}

			// Track when we're inside text-containing elements
			if isTextElement(t.Name.Local) {
				inTextElement = true
			}

		case xml.EndElement:
			if isTextElement(t.Name.Local) {
				inTextElement = false
			}

		case xml.CharData:
			if inTextElement {
				text := strings.TrimSpace(string(t))
				if text != "" {
					textBuilder.WriteString(text)
					textBuilder.WriteString(" ")
				}
			}
		}
	}

	// Count words
	text := textBuilder.String()
	if text != "" {
		words := strings.Fields(text)
		result.WordCount = len(words)
	}

	return result, nil
}

// isTextElement returns true if the element typically contains readable text
func isTextElement(name string) bool {
	switch name {
	case "P", "FP", "HD", "HEAD", "PRTPAGE", "EDNOTE", "NOTE", "EXTRACT", "APPRO":
		return true
	default:
		return false
	}
}

// calculateChecksum computes MD5 hash of content
func (p *Parser) calculateChecksum(content []byte) string {
	hash := md5.Sum(content)
	return hex.EncodeToString(hash[:])
}
