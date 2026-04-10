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

	"github.com/joho/godotenv"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

var basePath = "."

type DynamoDBDevice struct {
	Item struct {
		ID                    struct{ S string }
		Name                  struct{ S string }
		DeviceType            struct{ S string }
		ThingTypeName         struct{ S string }
		LocationId            struct{ S string }
		PartnerId             struct{ S string }
		ProductType           struct{ S string }
		TerminalSize          struct{ S string }
		NumSlots              struct{ S string }
		SalesId               struct{ S string }
		DeploymentDate        struct{ S string }
		DeviceAgentVersion    struct{ S string }
		OperatingHours        struct{ S string }
		SimCardNumber         struct{ S string }
		Traffic               struct{ S string }
		PowerbankAvailability struct{ S string }
		CreatedAt             struct{ S string }
		UpdatedAt             struct{ S string }

		// 🔥 STRING (AMAN)
		SlotManagement struct{ S string }
		ModuleStatus   struct{ S string }
	}
}

type DeviceLog struct {
	ID                    string `gorm:"column:id"`
	Name                  string `gorm:"column:name"`
	DeviceType            string `gorm:"column:device_type"`
	ThingTypeName         string `gorm:"column:thing_type_name"`
	LocationId            string `gorm:"column:location_id"`
	PartnerId             string `gorm:"column:partner_id"`
	ProductType           string `gorm:"column:product_type"`
	TerminalSize          string `gorm:"column:terminal_size"`
	NumSlots              string `gorm:"column:num_slots"`
	SalesId               string `gorm:"column:sales_id"`
	DeploymentDate        string `gorm:"column:deployment_date"`
	DeviceAgentVersion    string `gorm:"column:device_agent_version"`
	OperatingHours        string `gorm:"column:operating_hours"`
	SimCardNumber         string `gorm:"column:sim_card_number"`
	Traffic               string `gorm:"column:traffic"`
	PowerbankAvailability string `gorm:"column:powerbank_availability"`
	CreatedAt             string `gorm:"column:created_at"`
	UpdatedAt             string `gorm:"column:updated_at"`

	SlotManagement string `gorm:"column:slot_management;type:jsonb"`
	ModuleStatus   string `gorm:"column:module_status;type:jsonb"`
}

func (DeviceLog) TableName() string {
	return "device_log"
}

func main() {

	fmt.Println("🚀 START IMPORT DEVICE LOG")

	wd, _ := os.Getwd()
	fmt.Println("Working dir:", wd)

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
		fmt.Println("DB error:", err)
		return
	}

	db.AutoMigrate(&DeviceLog{})

	files, err := os.ReadDir(basePath)
	if err != nil {
		fmt.Println("Read dir error:", err)
		return
	}

	var jsonFiles []string
	for _, f := range files {
		if filepath.Ext(f.Name()) == ".json" {
			jsonFiles = append(jsonFiles, f.Name())
		}
	}

	fmt.Println("Found JSON files:", jsonFiles)

	if len(jsonFiles) == 0 {
		fmt.Println("❌ NO JSON FILE FOUND")
		return
	}

	wg := sync.WaitGroup{}
	wg.Add(len(jsonFiles))

	for _, fname := range jsonFiles {

		go func(filename string) {
			defer wg.Done()

			fmt.Println("📂 Processing:", filename)

			file, err := os.Open(filepath.Join(basePath, filename))
			if err != nil {
				fmt.Println("Open file error:", err)
				return
			}
			defer file.Close()

			scanner := bufio.NewScanner(file)
			scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

			var batch []DeviceLog
			success := 0
			failed := 0

			for scanner.Scan() {

				var d DynamoDBDevice
				line := scanner.Bytes()

				if err := json.Unmarshal(line, &d); err != nil {
					fmt.Println("JSON ERROR:", err)
					failed++
					continue
				}

				mapped := mapDevice(d)

				if mapped.ID == "" {
					failed++
					continue
				}

				batch = append(batch, mapped)
				success++

				if len(batch) >= 500 {
					insertBatch(db, batch)
					batch = nil
				}
			}

			if len(batch) > 0 {
				insertBatch(db, batch)
			}

			fmt.Printf("✅ DONE %s | SUCCESS: %d | FAILED: %d\n", filename, success, failed)

		}(fname)
	}

	wg.Wait()
	fmt.Println("🔥 ALL DONE")
}

func insertBatch(db *gorm.DB, batch []DeviceLog) {
	if err := db.CreateInBatches(batch, 500).Error; err != nil {
		fmt.Println("❌ BATCH ERROR:", err)

		// fallback insert satu-satu biar ga gagal semua
		for _, item := range batch {
			if err := db.Create(&item).Error; err != nil {
				fmt.Println("❌ ROW ERROR:", err)
			}
		}
	}
}

func mapDevice(d DynamoDBDevice) DeviceLog {

	return DeviceLog{
		ID:                    clean(d.Item.ID.S),
		Name:                  clean(d.Item.Name.S),
		DeviceType:            clean(d.Item.DeviceType.S),
		ThingTypeName:         clean(d.Item.ThingTypeName.S),
		LocationId:            clean(d.Item.LocationId.S),
		PartnerId:             clean(d.Item.PartnerId.S),
		ProductType:           clean(d.Item.ProductType.S),
		TerminalSize:          clean(d.Item.TerminalSize.S),
		NumSlots:              clean(d.Item.NumSlots.S),
		SalesId:               clean(d.Item.SalesId.S),
		DeploymentDate:        clean(d.Item.DeploymentDate.S),
		DeviceAgentVersion:    clean(d.Item.DeviceAgentVersion.S),
		OperatingHours:        clean(d.Item.OperatingHours.S),
		SimCardNumber:         clean(d.Item.SimCardNumber.S),
		Traffic:               clean(d.Item.Traffic.S),
		PowerbankAvailability: clean(d.Item.PowerbankAvailability.S),
		CreatedAt:             clean(d.Item.CreatedAt.S),
		UpdatedAt:             clean(d.Item.UpdatedAt.S),

		SlotManagement: safeJSON(d.Item.SlotManagement.S),
		ModuleStatus:   safeJSON(d.Item.ModuleStatus.S),
	}
}

func clean(val string) string {
	val = strings.TrimSpace(val)
	val = strings.ReplaceAll(val, `"`, "")
	return val
}

func safeJSON(val string) string {

	val = strings.TrimSpace(val)

	if val == "" {
		return "{}"
	}

	// parse langsung
	var js interface{}
	if err := json.Unmarshal([]byte(val), &js); err == nil {
		return val
	}

	//  unescape (double encoded)
	var unescaped string
	if err := json.Unmarshal([]byte(`"`+val+`"`), &unescaped); err == nil {
		if json.Unmarshal([]byte(unescaped), &js) == nil {
			return unescaped
		}
	}

	// fallback aman
	return "{}"
}
