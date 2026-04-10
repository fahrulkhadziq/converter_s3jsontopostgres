package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"sync"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

// Struct untuk menyimpan data dari JSON DynamoDB
type DynamoDBItem struct {
	Item struct {
		ID                 struct{ S string }
		UserId             struct{ S string }
		CreatedAt          struct{ S string }
		UpdatedAt          struct{ S string }
		CreatedBy          struct{ S string }
		UpdatedBy          struct{ S string }
		PbPackageId        struct{ S string }
		Quota              struct{ N string }
		LegacyUserId       struct{ S string }
		QuotaType          struct{ S string }
		ValidUntil         struct{ S string }
		Name               struct{ S string }
		Description        struct{ S string }
		ShortDescription   struct{ S string }
		DescriptionId      struct{ S string }
		ShortDescriptionId struct{ S string }
		Price              struct{ N string }
		Tag                struct{ S string }
	}
}

// Struct untuk menyimpan data dalam bentuk JSON biasa
type NormalJSONItem struct {
	ID                 string `gorm:"column:id"`
	UserId             string `gorm:"column:user_id"`
	CreatedAt          string `gorm:"column:created_at"`
	UpdatedAt          string `gorm:"column:updated_at"`
	CreatedBy          string `gorm:"column:created_by"`
	UpdatedBy          string `gorm:"column:updated_by"`
	PbPackageId        string `gorm:"column:pb_package_id"`
	Quota              int    `gorm:"column:quota"`
	LegacyUserId       string `gorm:"column:legacy_user_id"`
	QuotaType          string `gorm:"column:quota_type"`
	ValidUntil         string `gorm:"column:valid_until"`
	Name               string `gorm:"column:name"`
	Description        string `gorm:"column:description"`
	ShortDescription   string `gorm:"column:short_description"`
	DescriptionId      string `gorm:"column:description_id"`
	ShortDescriptionId string `gorm:"column:short_description_id"`
	Price              int    `gorm:"column:price"`
	Tag                string `gorm:"column:tag"`
}

func (NormalJSONItem) TableName() string {
	return "user_pb_package"
}

func main() {
	runtime.GOMAXPROCS(4)
	dsn := "user=postgres password=admin dbname=recharge host=localhost port=5432 sslmode=disable"
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		fmt.Println("Error connecting to the database:", err)
		return
	}

	// Migrate struktur tabel jika diperlukan
	db.AutoMigrate(&NormalJSONItem{})

	// Baca file JSON dari DynamoDB
	fileName := []string{
		"./7m7cfvut5a7n3eelcv4wfwol6i.json",
		"./oodxyetcxa3rxbngpuch5kx6ly.json",
		"./rz7g7omnrqzr7mdmmarxmwlr2y.json",
		"./xxeyho523m56xgr4s3erimlv7y.json",
	}
	wg := sync.WaitGroup{}
	wg.Add(len(fileName))
	for _, v := range fileName {
		file, err := os.Open(v)
		if err != nil {
			fmt.Println("Error opening file:", err)
			return
		}
		defer file.Close()

		go func(file *os.File, db *gorm.DB) {
			defer wg.Done()
			// Baca isi file
			scanner := bufio.NewScanner(file)
			for scanner.Scan() {
				var dynamoDBItem DynamoDBItem
				line := scanner.Bytes()
				if err := json.Unmarshal(line, &dynamoDBItem); err != nil {
					fmt.Println("Error decoding JSON:", err)
					return
				}

				// Sekarang Anda memiliki normalJSONItem yang berisi data dari setiap baris JSON
				// Anda dapat melakukan apapun yang Anda butuhkan di sini, misalnya menyimpan ke database
				// Buat objek JSON biasa

				normalJSONItem := NormalJSONItem{}
				normalJSONItem.ID = handleMissingString(dynamoDBItem.Item.ID.S)
				normalJSONItem.UserId = handleMissingString(dynamoDBItem.Item.UserId.S)
				normalJSONItem.CreatedAt = handleMissingString(dynamoDBItem.Item.CreatedAt.S)
				normalJSONItem.UpdatedAt = handleMissingString(dynamoDBItem.Item.UpdatedAt.S)
				normalJSONItem.CreatedBy = handleMissingString(dynamoDBItem.Item.CreatedBy.S)
				normalJSONItem.UpdatedBy = handleMissingString(dynamoDBItem.Item.UpdatedBy.S)
				normalJSONItem.PbPackageId = handleMissingString(dynamoDBItem.Item.PbPackageId.S)
				normalJSONItem.Quota = handleMissingInt(dynamoDBItem.Item.Quota.N)
				normalJSONItem.LegacyUserId = handleMissingString(dynamoDBItem.Item.LegacyUserId.S)
				normalJSONItem.QuotaType = handleMissingString(dynamoDBItem.Item.QuotaType.S)
				normalJSONItem.ValidUntil = handleMissingString(dynamoDBItem.Item.ValidUntil.S)
				normalJSONItem.Name = handleMissingString(dynamoDBItem.Item.Name.S)
				normalJSONItem.Description = handleMissingString(dynamoDBItem.Item.Description.S)
				normalJSONItem.ShortDescription = handleMissingString(dynamoDBItem.Item.ShortDescription.S)
				normalJSONItem.DescriptionId = handleMissingString(dynamoDBItem.Item.DescriptionId.S)
				normalJSONItem.ShortDescriptionId = handleMissingString(dynamoDBItem.Item.ShortDescriptionId.S)
				normalJSONItem.Price = handleMissingInt(dynamoDBItem.Item.Price.N)
				normalJSONItem.Tag = handleMissingString(dynamoDBItem.Item.Tag.S)

				// Simpan data ke database
				if err := db.Model(&NormalJSONItem{}).Create(&normalJSONItem).Error; err != nil {
					fmt.Println("Error inserting data into database:", err)
					return
				}
			}

			if err := scanner.Err(); err != nil {
				fmt.Println("Error reading file:", err)
				return
			}
		}(file, db)
	}
	fmt.Println("Waiting for all goroutines to finish...")
	wg.Wait()
	fmt.Println("All goroutines have finished.")
}

func handleMissingString(value string) string {
	if value == "" {
		return ""
	}
	return value
}

// Helper function to handle missing number values
func handleMissingNumber(value string) string {
	if value == "" {
		return "0"
	}
	return value
}

func handleMissingInt(value string) int {
	if value == "" {
		return 0
	}
	i, err := strconv.Atoi(value)
	if err != nil {
		fmt.Println("Error converting string to int:", err)
		return 0
	}
	return i
}

// Helper function to handle missing string array values
func handleMissingStringArray(value []string) string {
	if len(value) == 0 {
		return ""
	}
	return value[0]
}
