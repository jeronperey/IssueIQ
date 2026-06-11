package main

import (
	"fmt"

	"github.com/jeronperey/issueiq/shared/schema"
)

var allowedSources = map[string]bool{
	"github":  true,
	"zendesk": true,
	"mock":    true,
	"jira":    true,
}

func ValidateRawIssue(issue schema.RawIssue) error {
	if issue.SourceID == "" {
		return fmt.Errorf("source_id must not be empty")
	}
	if !allowedSources[issue.Source] {
		return fmt.Errorf("source %q is not allowed: must be one of github, zendesk, mock, jira", issue.Source)
	}
	if issue.RawPayload == nil {
		return fmt.Errorf("raw_payload must not be nil")
	}
	return nil
}
