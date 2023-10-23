package models

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
)

type EntryData struct {
	URL              string            `json:"url,omitempty"`
	Title            string            `json:"title,omitempty"`
	Keywords         []string          `json:"keywords,omitempty"` // used for passing keywords specified in source configuration (ex. youtube channel on golang could have enforced tags 'youtube', 'video', 'go'
	PublishedAt      time.Time         `json:"published_at,omitempty"`
	UpdatedAt        time.Time         `json:"updated_at,omitempty"`
	Engagement       int64             `json:"engagement,omitempty"`         // value obtained by engagement threshold checks, can be calculated differently for different resource types
	OriginID         string            `json:"origin_id,omitempty"`          // entry id at origin resource if any
	FreeformContent  string            `json:"freeform_content,omitempty"`   // text content posted directly to daily.dev
	Cleaned          map[string]string `json:"cleaned,omitempty"`            // cleaned content {cleaner:location, ...}
	ScrapedContentID string            `json:"scraped_content_id,omitempty"` // gcs location of scraped content (if any)
	// scraped data, common for all kinds of content
}

func (ed EntryData) Value() (driver.Value, error) {
	return json.Marshal(ed)
}

func (ed *EntryData) Scan(value interface{}) error {
	b, ok := value.([]byte)
	if !ok {
		return errors.New("entry data assertion to []byte failed")
	}

	return json.Unmarshal(b, ed)
}

type Enriched struct {
	Model    string `json:"model,omitempty"`
	EnrichID string `json:"enrich_id,omitempty"`
}

type EntryMetadata struct {
	SubmissionID string    `json:"submission_id,omitempty"`
	Origin       string    `json:"origin,omitempty"`  // initiator of scraping: internal/community/squad
	PostID       string    `json:"post_id,omitempty"` // post ID from API, if post was created there and passed for scraping
	Order        int       `json:"order,omitempty"`
	Enriched     *Enriched `json:"enriched,omitempty"`
	// completeness stages if there is more than one
}

func (md EntryMetadata) Value() (driver.Value, error) {
	return json.Marshal(md)
}

func (md *EntryMetadata) Scan(value interface{}) error {
	b, ok := value.([]byte)
	if !ok {
		return errors.New("entry meta assertion to []byte failed")
	}

	return json.Unmarshal(b, md)
}

type ContentEntry struct {
	ID            uuid.UUID       `gorm:"column:id;type:uuid" json:"id"`
	EngineID      string          `gorm:"column:engine_id" json:"engine_id"`
	SourceID      string          `gorm:"column:source_id" json:"source_id"`
	Status        string          `gorm:"column:status" json:"status"`
	EntryType     string          `gorm:"column:entry_type" json:"entry_type"`
	EntryData     EntryData       `gorm:"column:entry_data;type:jsonb" json:"entry_data"`                   // data related to content or resource where it is located
	EntryMetadata EntryMetadata   `gorm:"column:entry_metadata;type:jsonb" json:"entry_metadata,omitempty"` // additional data about entry in context of our system - like repost, sumbission id, origin.
	Options       json.RawMessage `gorm:"column:options;type:jsonb" json:"options,omitempty"`               // processing options override
	Payload       json.RawMessage `gorm:"column:payload;type:jsonb" json:"payload,omitempty"`               // scraped data
	CreatedAt     time.Time       `gorm:"column:created_at" json:"created_at"`
	UpdatedAt     time.Time       `gorm:"column:updated_at" json:"updated_at"`
	RejectReason  string          `gorm:"column:reject_reason" json:"reject_reason,omitempty"`
}

func (c ContentEntry) TableName() string {
	return "content_entry"
}
