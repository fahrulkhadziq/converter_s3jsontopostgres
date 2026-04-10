package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/joho/godotenv"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type DynamoDBItemLocation struct {
	Item struct {
		ID            struct{ S string }
		Name          struct{ S string }
		Subname       struct{ S string }
		Address       struct{ S string }
		CityOrRegency struct{ S string }
		District      struct{ S string }
		Province      struct{ S string }
		PostalCode    struct{ S string }
		Latitude      struct{ S string }
		Longitude     struct{ S string }
		LocationType  struct{ S string }
		Description   struct{ S string }
		Owner         struct{ S string }
		CreatedAt     struct{ S string }
		UpdatedAt     struct{ S string }
		Tags          struct{ S string }
		MachineType   struct{ S string }
	}
}

type NormalJSONItemLocation struct {
	ID            string `gorm:"column:id"`
	Name          string `gorm:"column:name"`
	Subname       string `gorm:"column:subname"`
	Address       string `gorm:"column:address"`
	CityOrRegency string `gorm:"column:city_or_regency"`
	District      string `gorm:"column:district"`
	Province      string `gorm:"column:province"`
	PostalCode    string `gorm:"column:postal_code"`
	Latitude      string `gorm:"column:latitude"`
	Longitude     string `gorm:"column:longitude"`
	LocationType  string `gorm:"column:location_type"`
	Description   string `gorm:"column:description"`
	Owner         string `gorm:"column:owner"`
	CreatedAt     string `gorm:"column:created_at"`
	UpdatedAt     string `gorm:"column:updated_at"`
	Tags          string `gorm:"column:tags"`
	MachineType   string `gorm:"column:machine_type"`
}

func (NormalJSONItemLocation) TableName() string {
	return "location"
}

func main() {

	err := godotenv.Load()
	if err != nil {
		log.Println("No .env file found")
	}

	// ambil env
	user := os.Getenv("DB_USER")
	pass := os.Getenv("DB_PASSWORD")
	name := os.Getenv("DB_NAME")
	host := os.Getenv("DB_HOST")
	port := os.Getenv("DB_PORT")
	ssl := os.Getenv("DB_SSLMODE")

	dsn := fmt.Sprintf("user=%s password=%s dbname=%s host=%s port=%s sslmode=%s", user, pass, name, host, port, ssl)
	fmt.Println("DSN:", dsn)

	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Warn),
	})
	if err != nil {
		fmt.Println("DB connection error:", err)
		return
	}

	sqlDB, _ := db.DB()
	sqlDB.SetMaxIdleConns(100)
	sqlDB.SetMaxOpenConns(500)
	sqlDB.SetConnMaxLifetime(time.Hour)

	// auto create table
	db.AutoMigrate(&NormalJSONItemLocation{})

	files, err := os.ReadDir(".")
	if err != nil {
		fmt.Println("Read dir error:", err)
		return
	}

	var jsonFiles []string
	for _, f := range files {
		if !f.IsDir() && filepath.Ext(f.Name()) == ".json" {
			jsonFiles = append(jsonFiles, f.Name())
		}
	}

	fmt.Println("Found files:", jsonFiles)

	wg := sync.WaitGroup{}
	wg.Add(len(jsonFiles))

	for _, fname := range jsonFiles {

		go func(filename string) {
			defer wg.Done()

			file, err := os.Open(filename)
			if err != nil {
				fmt.Println("Open file error:", err)
				return
			}
			defer file.Close()

			scanner := bufio.NewScanner(file)
			buf := make([]byte, 0, 64*1024)
			scanner.Buffer(buf, 1024*1024)

			var batch []NormalJSONItemLocation
			batchSize := 500

			for scanner.Scan() {

				var dynamo DynamoDBItemLocation
				line := scanner.Bytes()

				if err := json.Unmarshal(line, &dynamo); err != nil {
					fmt.Println("JSON decode error:", err)
					continue
				}

				normal := mapToLocation(dynamo)
				batch = append(batch, normal)

				if len(batch) >= batchSize {
					insertBatch(db, batch)
					batch = nil
				}
			}

			if len(batch) > 0 {
				insertBatch(db, batch)
			}

			if err := scanner.Err(); err != nil {
				fmt.Println("Scanner error:", err)
			}

			fmt.Println("Done:", filename)

		}(fname)
	}

	fmt.Println("Processing...")
	wg.Wait()
	fmt.Println("ALL DONE 🚀")
}

func mapToLocation(d DynamoDBItemLocation) NormalJSONItemLocation {

	return NormalJSONItemLocation{
		ID:            cleanString(d.Item.ID.S),
		Name:          cleanString(d.Item.Name.S),
		Subname:       cleanString(d.Item.Subname.S),
		Address:       cleanString(d.Item.Address.S),
		CityOrRegency: cleanString(d.Item.CityOrRegency.S),
		District:      cleanString(d.Item.District.S),
		Province:      cleanString(d.Item.Province.S),
		PostalCode:    cleanString(d.Item.PostalCode.S),
		Latitude:      cleanCoordinate(d.Item.Latitude.S),
		Longitude:     cleanCoordinate(d.Item.Longitude.S),
		LocationType:  cleanString(d.Item.LocationType.S),
		Description:   cleanString(d.Item.Description.S),
		Owner:         cleanString(d.Item.Owner.S),
		CreatedAt:     cleanString(d.Item.CreatedAt.S),
		UpdatedAt:     cleanString(d.Item.UpdatedAt.S),
		Tags:          cleanString(d.Item.Tags.S),
		MachineType:   cleanString(d.Item.MachineType.S),
	}
}

func insertBatch(db *gorm.DB, data []NormalJSONItemLocation) {
	if err := db.CreateInBatches(data, 500).Error; err != nil {
		fmt.Println("Insert error:", err)
	}
}

func cleanString(val string) string {
	val = strings.TrimSpace(val)
	val = strings.ReplaceAll(val, `"`, "")
	return val
}

// 🔥 HANDLE DATA KOTOR PARAH
func cleanCoordinate(val string) string {

	val = strings.TrimSpace(val)
	val = strings.ReplaceAll(val, `"`, "")

	// split kalau ada banyak value
	parts := strings.Fields(val)

	if len(parts) > 0 {
		val = parts[0]
	}

	// buang karakter aneh
	val = strings.Trim(val, ".")
	val = strings.ReplaceAll(val, "--", "-")

	// validasi minimal harus ada angka
	if !containsNumber(val) {
		return ""
	}

	return val
}

func containsNumber(s string) bool {
	for _, c := range s {
		if c >= '0' && c <= '9' {
			return true
		}
	}
	return false
}
