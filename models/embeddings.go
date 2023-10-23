package models

import (
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"
)

type Embeddings struct {
	ID        uuid.UUID       `gorm:"column:id;type:uuid" json:"id"`
	EntryID   uuid.UUID       `gorm:"column:entry_id;type:uuid" json:"entry_id"`
	Embedding pq.Float32Array `gorm:"column:embedding;type:real[]" json:"embedding"`
	Type      string          `gorm:"column:type" json:"type"`       // provider, model and kind of content used to generate embedding like "azure_ada2_title_summary"
	Content   string          `gorm:"column:content" json:"content"` // original content used to generate embedding
	CreatedAt time.Time       `gorm:"column:created_at" json:"created_at"`
}

func (e Embeddings) TableName() string {
	return "embeddings"
}
