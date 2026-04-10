package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/joho/godotenv"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// Struct untuk menyimpan data dari JSON DynamoDB
type DynamoDBItem struct {
	Item struct {
		ID                          struct{ S string }
		Email                       struct{ S string }
		PhoneNumber                 struct{ S string }
		PreferredUsername           struct{ S string }
		TempPassword                struct{ S string }
		Balance                     struct{ N string }
		ReferralCode                struct{ S string }
		ReferralRank                struct{ S string }
		ReferalCode                 struct{ S string } // DEPRECATED
		PaidLifeline                struct{ N string }
		FreeLifeline                struct{ N string }
		PromoId                     struct{ SS []string }
		PackageId                   struct{ SS []string }
		MultiRent                   struct{ BOOL bool }
		AvatarURL                   struct{ S string }
		Name                        struct{ S string }
		FirstName                   struct{ S string }
		LastName                    struct{ S string }
		IsConnectedToSocialProvider struct{ BOOL bool }
		SocialProviderName          struct{ S string }
		AccountStatus               struct{ S string }
		Gender                      struct{ S string }
		Address                     struct{ S string }
		Birthdate                   struct{ S string }
		CreatedAt                   struct{ S string }
		LastActivity                struct{ S string }
		UpdatedAt                   struct{ S string }
		CreatedBy                   struct{ S string }
		UpdatedBy                   struct{ S string }
		Tags                        struct{ S string }
		IsEmployee                  struct{ BOOL bool }
		DeviceID                    struct{ S string }
		AdvertisingID               struct{ S string }
		LDUserID                    struct{ S string }
		TierUser                    struct{ S string }
		LinkedAt                    struct{ S string }
		CompliteProfileAt           struct{ S string }
		ExpiredBalanceAt            struct{ S string }
		FirstTransactionAt          struct{ S string }
		LastTransactionAt           struct{ S string } // lastTransactionBalanceAt
		LastTransactionPackageAt    struct{ S string }
		RentCounter                 struct{ N string }
		LastChangeEmail             struct{ S string }
		LastChangePhoneNumber       struct{ S string }
	}
}

// id: ID
// # Secondary index
// email: String
// phoneNumber: String
// preferredUsername: String
// tempPassword: String # just in case needed
// balance: Int
// referralCode: String
// referralRank: String
// referalCode: String # DEPRECATED
// paidLifeline: Int
// freeLifeline: Int
// # freeLifelineId: [String] # at the first stage, this will hit existing database
// promoId: [String] # at the first stage, this will hit existing database
// packageId: [String] # at the first stage, this will hit existing database
// multiRent: Boolean
// # Profile section
// avatarUrl: String
// name: String
// firstName: String
// lastName: String
// isConnectedToSocialProvider: Boolean
// socialProviderName: String
// accountStatus: String
// gender: String
// address: String
// birthdate: AWSDateTime
// createdAt: AWSDateTime
// lastActivity: AWSDateTime
// updatedAt: AWSDateTime
// createdBy: String
// updatedBy: String
// tags: String
// isEmployee: Boolean
// deviceId: String
// advertisingId: String
// ldUserId: String
// tierUser: String
// linkedAt: AWSDateTime
// compliteProfileAt: AWSDateTime
// expiredBalanceAt: String
// firstTransactionAt: AWSDateTime
// lastTransactionAt: String #lastTransactionBalanceAt
// lastTransactionPackageAt: AWSDateTime
// rentCounter: Int
// lastChangeEmail: AWSDateTime
// lastChangePhoneNumber: AWSDateTime
// Struct untuk menyimpan data dalam bentuk JSON biasa
type NormalJSONItem struct {
	ID                          string `gorm:"column:id"`
	Email                       string `gorm:"column:email"`
	PhoneNumber                 string `gorm:"column:phone_number"`
	PreferredUsername           string `gorm:"column:preferred_username"`
	TempPassword                string `gorm:"column:temp_password"`
	Balance                     string `gorm:"column:balance"`
	ReferralCode                string `gorm:"column:referral_code"`
	ReferralRank                string `gorm:"column:referral_rank"`
	PaidLifeline                string `gorm:"column:paid_lifeline"`
	FreeLifeline                string `gorm:"column:free_lifeline"`
	PromoID                     string `gorm:"column:promo_id"`
	PackageID                   string `gorm:"column:package_id"`
	MultiRent                   bool   `gorm:"column:multi_rent"`
	AvatarURL                   string `gorm:"column:avatar_url"`
	Name                        string `gorm:"column:name"`
	FirstName                   string `gorm:"column:first_name"`
	LastName                    string `gorm:"column:last_name"`
	IsConnectedToSocialProvider bool   `gorm:"column:is_connected_to_social_provider"`
	SocialProviderName          string `gorm:"column:social_provider_name"`
	AccountStatus               string `gorm:"column:account_status"`
	Gender                      string `gorm:"column:gender"`
	Address                     string `gorm:"column:address"`
	Birthdate                   string `gorm:"column:birthdate"`
	CreatedAt                   string `gorm:"column:created_at"`
	LastActivity                string `gorm:"column:last_activity"`
	UpdatedAt                   string `gorm:"column:updated_at"`
	CreatedBy                   string `gorm:"column:created_by"`
	UpdatedBy                   string `gorm:"column:updated_by"`
	IsEmployee                  bool   `gorm:"column:is_employee"`
	DeviceID                    string `gorm:"column:device_id"`
	AdvertisingID               string `gorm:"column:advertising_id"`
	LDUserID                    string `gorm:"column:ld_user_id"`
	TierUser                    string `gorm:"column:tier_user"`
	LinkedAt                    string `gorm:"column:linked_at"`
	CompliteProfileAt           string `gorm:"column:complite_profile_at"`
	ExpiredBalanceAt            string `gorm:"column:expired_balance_at"`
	FirstTransactionAt          string `gorm:"column:first_transaction_at"`
	LastTransactionAt           string `gorm:"column:last_transaction_at"`
	LastTransactionBalanceAt    string `gorm:"column:last_transaction_balance_at"`
	LastTransactionPackageAt    string `gorm:"column:last_transaction_package_at"`
	RentCounter                 string `gorm:"column:rent_counter"`
	LastChangeEmail             string `gorm:"column:last_change_email"`
	LastChangePhoneNumber       string `gorm:"column:last_change_phone_number"`
}

func (NormalJSONItem) TableName() string {
	return "user_profiles"
}

func main() {
	// runtime.GOMAXPROCS(4)
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
	// db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	// if err != nil {
	// 	fmt.Println("Error connecting to the database:", err)
	// 	return
	// }

	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Warn), // Set logger level (optional)
	})

	if err != nil {
		fmt.Println("Error connecting to the database:", err)
		return
	}

	// Set connection pool settings
	sqlDB, err := db.DB()
	if err != nil {
		fmt.Println("Error getting database instance:", err)
		return
	}

	// Set the maximum number of connections in the idle connection pool.
	sqlDB.SetMaxIdleConns(1000)

	// Set the maximum number of open connections to the database.
	sqlDB.SetMaxOpenConns(10000)

	// Set the maximum lifetime of a connection.
	sqlDB.SetConnMaxLifetime(time.Hour)

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
				normalJSONItem.Email = handleMissingString(dynamoDBItem.Item.Email.S)
				normalJSONItem.PhoneNumber = handleMissingString(dynamoDBItem.Item.PhoneNumber.S)
				normalJSONItem.PreferredUsername = handleMissingString(dynamoDBItem.Item.PreferredUsername.S)
				normalJSONItem.TempPassword = handleMissingString(dynamoDBItem.Item.TempPassword.S)
				normalJSONItem.Balance = handleMissingNumber(dynamoDBItem.Item.Balance.N)
				normalJSONItem.ReferralCode = handleMissingString(dynamoDBItem.Item.ReferralCode.S)
				normalJSONItem.ReferralRank = handleMissingString(dynamoDBItem.Item.ReferralRank.S)
				normalJSONItem.PaidLifeline = handleMissingNumber(dynamoDBItem.Item.PaidLifeline.N)
				normalJSONItem.FreeLifeline = handleMissingNumber(dynamoDBItem.Item.FreeLifeline.N)
				// Assuming promo_id and package_id are joined string arrays separated by ","
				normalJSONItem.PromoID = handleMissingStringArray(dynamoDBItem.Item.PromoId.SS)
				normalJSONItem.PackageID = handleMissingStringArray(dynamoDBItem.Item.PackageId.SS)
				normalJSONItem.MultiRent = dynamoDBItem.Item.MultiRent.BOOL
				normalJSONItem.AvatarURL = handleMissingString(dynamoDBItem.Item.AvatarURL.S)
				normalJSONItem.Name = handleMissingString(dynamoDBItem.Item.Name.S)
				normalJSONItem.FirstName = handleMissingString(dynamoDBItem.Item.FirstName.S)
				normalJSONItem.LastName = handleMissingString(dynamoDBItem.Item.LastName.S)
				normalJSONItem.IsConnectedToSocialProvider = dynamoDBItem.Item.IsConnectedToSocialProvider.BOOL
				normalJSONItem.SocialProviderName = handleMissingString(dynamoDBItem.Item.SocialProviderName.S)
				normalJSONItem.AccountStatus = handleMissingString(dynamoDBItem.Item.AccountStatus.S)
				normalJSONItem.Gender = handleMissingString(dynamoDBItem.Item.Gender.S)
				normalJSONItem.Address = handleMissingString(dynamoDBItem.Item.Address.S)
				normalJSONItem.Birthdate = handleMissingString(dynamoDBItem.Item.Birthdate.S)
				normalJSONItem.CreatedAt = handleMissingString(dynamoDBItem.Item.CreatedAt.S)
				normalJSONItem.LastActivity = handleMissingString(dynamoDBItem.Item.LastActivity.S)
				normalJSONItem.UpdatedAt = handleMissingString(dynamoDBItem.Item.UpdatedAt.S)
				normalJSONItem.CreatedBy = handleMissingString(dynamoDBItem.Item.CreatedBy.S)
				normalJSONItem.UpdatedBy = handleMissingString(dynamoDBItem.Item.UpdatedBy.S)
				normalJSONItem.IsEmployee = dynamoDBItem.Item.IsEmployee.BOOL
				normalJSONItem.DeviceID = handleMissingString(dynamoDBItem.Item.DeviceID.S)
				normalJSONItem.AdvertisingID = handleMissingString(dynamoDBItem.Item.AdvertisingID.S)
				normalJSONItem.LDUserID = handleMissingString(dynamoDBItem.Item.LDUserID.S)
				normalJSONItem.TierUser = handleMissingString(dynamoDBItem.Item.TierUser.S)
				normalJSONItem.LinkedAt = handleMissingString(dynamoDBItem.Item.LinkedAt.S)
				normalJSONItem.CompliteProfileAt = handleMissingString(dynamoDBItem.Item.CompliteProfileAt.S)
				normalJSONItem.ExpiredBalanceAt = handleMissingString(dynamoDBItem.Item.ExpiredBalanceAt.S)
				normalJSONItem.FirstTransactionAt = handleMissingString(dynamoDBItem.Item.FirstTransactionAt.S)
				normalJSONItem.LastTransactionAt = handleMissingString(dynamoDBItem.Item.LastTransactionAt.S)
				normalJSONItem.LastTransactionBalanceAt = handleMissingString(dynamoDBItem.Item.LastTransactionAt.S)
				normalJSONItem.LastTransactionPackageAt = handleMissingString(dynamoDBItem.Item.LastTransactionPackageAt.S)
				normalJSONItem.RentCounter = handleMissingNumber(dynamoDBItem.Item.RentCounter.N)
				normalJSONItem.LastChangeEmail = handleMissingString(dynamoDBItem.Item.LastChangeEmail.S)
				normalJSONItem.LastChangePhoneNumber = handleMissingString(dynamoDBItem.Item.LastChangePhoneNumber.S)
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
