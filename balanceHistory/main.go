package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

// Struct untuk menyimpan data dari JSON DynamoDB
type DynamoDBItem struct {
	Item struct {
		ID        struct{ S string }
		Balance   struct{ N string }
		CreatedAt struct{ S string }
		UpdatedAt struct{ S string }
		UserID    struct{ S string }
	}
}

// Struct untuk menyimpan data dalam bentuk JSON biasa
type NormalJSONItem struct {
	ID        string `gorm:"column:id"`
	Balance   string `gorm:"column:balance"`
	CreatedAt string `gorm:"column:created_at"`
	UpdatedAt string `gorm:"column:updated_at"`
	UserId    string `gorm:"column:user_id"`
}

func (NormalJSONItem) TableName() string {
	return "balance_history"
}

func main() {
	runtime.GOMAXPROCS(4)
	dsn := "user=postgres password=p4ssw0rd dbname=recharge host=localhost port=5432 sslmode=disable"
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		fmt.Println("Error connecting to the database:", err)
		return
	}

	// Migrate struktur tabel jika diperlukan
	db.AutoMigrate(&NormalJSONItem{})

	// Baca file JSON dari DynamoDB
	files, err := os.ReadDir(".")
	if err != nil {
		fmt.Println("Error reading directory:", err)
		return
	}

	fileName := []string{}
	for _, file := range files {
		if !file.IsDir() && filepath.Ext(file.Name()) == ".json" {
			fileName = append(fileName, file.Name())
		}
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
				normalJSONItem.Balance = handleMissingNumber(dynamoDBItem.Item.Balance.N)
				normalJSONItem.CreatedAt = handleMissingString(dynamoDBItem.Item.CreatedAt.S)
				normalJSONItem.UpdatedAt = handleMissingString(dynamoDBItem.Item.UpdatedAt.S)
				normalJSONItem.UserId = handleMissingString(dynamoDBItem.Item.UserID.S)
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
