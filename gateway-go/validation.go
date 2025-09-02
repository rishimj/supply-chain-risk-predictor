package main

import (
	"errors"
	"strings"
	"time"
)

// validateNewsRequest validates the incoming news request
func validateNewsRequest(req *NewsRequest) error {
	// Check required fields
	if req.Headline == "" {
		return errors.New("headline is required")
	}
	
	if req.URL == "" {
		return errors.New("url is required")
	}
	
	if req.Published == "" {
		return errors.New("published is required")
	}
	
	// Validate published date format (ISO-8601)
	if _, err := time.Parse(time.RFC3339, req.Published); err != nil {
		return errors.New("published must be valid ISO-8601 format")
	}
	
	return nil
}

// sanitizeInput cleans and trims input strings
func sanitizeInput(input string) string {
	return strings.TrimSpace(input)
}