package main

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/caarlos0/env/v9"
	"github.com/google/uuid"
	"github.com/joho/godotenv"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/denisb0/import_embeddings/models"
)

const embeddingSize = 1536

func panicOnError(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func getDBConn() (*gorm.DB, error) {
	if err := godotenv.Load(".env.local"); err != nil {
		return nil, err
	}

	type Config struct {
		DBHost     string `env:"DB_HOST,required"`
		DBPort     string `env:"DB_PORT" envDefault:"5432"`
		DBUser     string `env:"DB_USER,required"`
		DBPassword string `env:"DB_PASSWORD,required"`
		DBName     string `env:"DB_NAME,required"`
	}

	var cfg Config
	if err := env.Parse(&cfg); err != nil {
		return nil, err
	}

	dsn := fmt.Sprintf("host=%s user=%s password=%s dbname=%s port=%s application_name=%s",
		cfg.DBHost, cfg.DBUser, cfg.DBPassword, cfg.DBName, cfg.DBPort, "yggdrasil")

	return gorm.Open(postgres.Open(dsn), &gorm.Config{})
}

type verifyError struct {
	Line           int
	Position       int
	OriginalValue  string
	ConvertedValue string
}

func verify(f *os.File) ([]verifyError, error) {
	csvReader := csv.NewReader(f)
	_, err := csvReader.Read()
	if err != nil {
		return nil, fmt.Errorf("unable to parse file as CSV %w", err)
	}

	resp := make([]verifyError, 0)

	var linesCount int
	vectorBuffer := make([]float32, embeddingSize)

	for {
		record, err := csvReader.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("unable to parse file as CSV %w", err)
		}

		input := record[0]
		input = strings.Trim(input, "[]")
		strValues := strings.Split(input, ", ")
		if len(strValues) != embeddingSize {
			return nil, fmt.Errorf("vector size not equal embedding values size: %d, line: %d", len(strValues), linesCount)
		}

		var valuesChecked int
		for i, strValue := range strValues {
			value, err := strconv.ParseFloat(strValue, 32)
			if err != nil {
				return nil, fmt.Errorf("error parsing value: %v, line %d, position %d", err, linesCount, i)
			}

			vectorBuffer[i] = float32(value)

			// compare
			controlStr := strconv.FormatFloat(value, 'g', -1, 64)
			if strValue != controlStr {
				resp = append(resp, verifyError{
					Line:           linesCount,
					Position:       i,
					OriginalValue:  strValue,
					ConvertedValue: controlStr,
				})
			}
			valuesChecked++
		}

		linesCount++

		if linesCount > 10 {
			break
		}
	}

	fmt.Println("lines count: ", linesCount)

	return resp, nil
}

func convertEmbedding(strEmbedding string, vectorBuffer []float32) error {
	strEmbedding = strings.Trim(strEmbedding, "[]")
	strValues := strings.Split(strEmbedding, ", ")

	if len(strValues) != embeddingSize {
		return fmt.Errorf("vector size not equal embedding values size: %d", len(strValues))
	}

	for i, strValue := range strValues {
		value, err := strconv.ParseFloat(strValue, 32)
		if err != nil {
			return fmt.Errorf("error parsing value: %v, position %d", err, i)
		}

		vectorBuffer[i] = float32(value)
	}

	return nil
}

func convertRecord(record []string, now time.Time) (models.Embeddings, error) {
	// header:  [embedding url content type]
	buf := make([]float32, embeddingSize)
	err := convertEmbedding(record[0], buf)
	if err != nil {
		return models.Embeddings{}, err
	}

	return models.Embeddings{
		Embedding: buf,
		Type:      record[3],
		Content:   record[2],
		CreatedAt: now,
	}, nil
}

func findEntryByURL(db *gorm.DB, url string) (uuid.UUID, error) {
	var entry models.ContentEntry
	if err := db.Model(&models.ContentEntry{}).Where("entry_data->>'url' = ?", url).Take(&entry).Error; err != nil {
		return uuid.UUID{}, err
	}

	return entry.ID, nil
}

func addEmbedding(db *gorm.DB, embedding models.Embeddings) error {
	return db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "id"}},
		DoNothing: true,
	}).Create(embedding).Error
}

func embeddingExists(db *gorm.DB, entryID uuid.UUID) bool {
	var data models.Embeddings
	err := db.Take(&data, "entry_id = ?", entryID).Error
	return err == nil
}

func dump(f *os.File, db *gorm.DB, now time.Time) error {
	// try with local db first
	csvReader := csv.NewReader(f)
	_, err := csvReader.Read()
	if err != nil {
		return fmt.Errorf("unable to parse file as CSV %w", err)
	}

	var recordCount int

	for {
		record, err := csvReader.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return fmt.Errorf("unable to parse file as CSV %w", err)
		}

		entryID, err := findEntryByURL(db, record[1])
		if err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				log.Println("record url not found ", record[1])
				continue
			}
			return fmt.Errorf("find entry error: %w", err)
		}

		if embeddingExists(db, entryID) {
			log.Println("embedding exists for id ", entryID)
			continue
		}

		emb, err := convertRecord(record, now)
		if err != nil {
			return fmt.Errorf("record convert error: %w", err)
		}

		emb.EntryID = entryID
		emb.ID = uuid.New()

		// j, _ := json.MarshalIndent(emb, "", "\t")
		// fmt.Println(string(j))

		if err := addEmbedding(db, emb); err != nil {
			return fmt.Errorf("record write error: %w", err)
		}

		recordCount++

		// if recordCount >= 3 {
		// 	break
		// }
	}

	return nil
}

func main() {
	db, err := getDBConn()
	panicOnError(err)

	f, err := os.Open("embedding.csv")
	panicOnError(err)

	defer func() {
		if err := f.Close(); err != nil {
			log.Println("error closing file", err)
		}
	}()

	// resp, err := verify(f)
	// panicOnError(err)

	panicOnError(dump(f, db, time.Now().UTC()))

	fmt.Println("processing complete")
}
