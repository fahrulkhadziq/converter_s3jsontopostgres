package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"sync"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// Struct untuk menyimpan data dari JSON DynamoDB
type DynamoDBItemPurchase struct {
	Item struct {
		ID                        struct{ S string }
		UserID                    struct{ S string }
		UserPhoneNumber           struct{ S string }
		CreatedAt                 struct{ S string }
		UpdatedAt                 struct{ S string }
		CreatedBy                 struct{ S string }
		UpdatedBy                 struct{ S string }
		Amount                    struct{ N string }
		DiscountAmount            struct{ N string }
		PurchaseType              struct{ S string }
		Status                    struct{ S string }
		ReferenceNumber           struct{ S string }
		PaymentGatewayName        struct{ S string }
		AdminFee                  struct{ N string }
		DeviceID                  struct{ S string }
		InvoiceNumber             struct{ S string }
		PromoName                 struct{ S string }
		PromoID                   struct{ S string }
		PromoAmount               struct{ N string }
		NormalAmount              struct{ N string }
		ProductID                 struct{ S string }
		ProductType               struct{ S string }
		ProductName               struct{ S string }
		MetaRequestPayment        struct{ S string }
		MetaResponsePayment       struct{ S string }
		MetaPaymentNotification   struct{ S string }
		ProductMetadata           struct{ S string }
		Tags                      struct{ S string }
		CouponMetadata            struct{ S string }
		PowerbankPackagesMetadata struct{ S string }
		UserPbPackageMetadata     struct{ S string }
		VoucherMasterID           struct{ S string }
		VoucherDetailID           struct{ S string }
		VoucherMasterMetadata     struct{ S string }
		VoucherDetailMetadata     struct{ S string }
		LoyaltyAmount             struct{ N string }
		Owner                     struct{ S string }
	}
}

// id: ID!
// userId: String
// userPhoneNumber: String
// createdAt: AWSDateTime
// updatedAt: AWSDateTime
// createdBy: String
// updatedBy: String
// # ----------
// amount: Int
// discountAmount: Int
// purchaseType: String # -- /BALANCE/PACKAGE/ORDER
// status: String # -- /SUCCESS/FAILED/EXPIRED
// referenceNumber: String
// paymentGatewayName: String # -- /MIDTRANS/DANA/MEGA/OVO/TCASH
// adminFee: Int # -- admin fee (date add: 2023-07-11)
// # ----------
// deviceId: String
// invoiceNumber: String
// # ----------
// promoName: String
// promoId: String
// promoAmount: Int
// normalAmount: Int
// # ----------
// productId: String
// productType: String
// productName: String
// # ----------
// metaRequestPayment: String
// metaResponsePayment: String
// metaPaymentNotification: String
// productMetadata: String
// tags: String
// # ----------
// couponMetadata: String
// powerbankPackagesMetadata: String
// userPbPackageMetadata: String
// # -----------
// voucherMasterId: String
// voucherDetailId: String
// voucherMasterMetadata: String
// voucherDetailMetadata: String
// loyaltyAmount: Int
// # Authorization
// owner: String # user id
// roGroups: [String] # gateway name
// rwGroups: [String]

// Struct untuk menyimpan data dalam bentuk JSON biasa
type NormalJSONItemPurchase struct {
	ID                        string `gorm:"column:id"`
	UserID                    string `gorm:"column:user_id"`
	UserPhoneNumber           string `gorm:"column:user_phone_number"`
	CreatedAt                 string `gorm:"column:created_at"`
	UpdatedAt                 string `gorm:"column:updated_at"`
	CreatedBy                 string `gorm:"column:created_by"`
	UpdatedBy                 string `gorm:"column:updated_by"`
	Amount                    int    `gorm:"column:amount"`
	DiscountAmount            int    `gorm:"column:discount_amount"`
	PurchaseType              string `gorm:"column:purchase_type"`
	Status                    string `gorm:"column:status"`
	ReferenceNumber           string `gorm:"column:reference_number"`
	PaymentGatewayName        string `gorm:"column:payment_gateway_name"`
	AdminFee                  int    `gorm:"column:admin_fee"`
	DeviceID                  string `gorm:"column:device_id"`
	InvoiceNumber             string `gorm:"column:invoice_number"`
	PromoName                 string `gorm:"column:promo_name"`
	PromoID                   string `gorm:"column:promo_id"`
	PromoAmount               int    `gorm:"column:promo_amount"`
	NormalAmount              int    `gorm:"column:normal_amount"`
	ProductID                 string `gorm:"column:product_id"`
	ProductType               string `gorm:"column:product_type"`
	ProductName               string `gorm:"column:product_name"`
	MetaRequestPayment        string `gorm:"column:meta_request_payment"`
	MetaResponsePayment       string `gorm:"column:meta_response_payment"`
	MetaPaymentNotification   string `gorm:"column:meta_payment_notification"`
	ProductMetadata           string `gorm:"column:product_metadata"`
	Tags                      string `gorm:"column:tags"`
	CouponMetadata            string `gorm:"column:coupon_metadata"`
	PowerbankPackagesMetadata string `gorm:"column:powerbank_packages_metadata"`
	UserPbPackageMetadata     string `gorm:"column:user_pb_package_metadata"`
	VoucherMasterID           string `gorm:"column:voucher_master_id"`
	VoucherDetailID           string `gorm:"column:voucher_detail_id"`
	VoucherMasterMetadata     string `gorm:"column:voucher_master_metadata"`
	VoucherDetailMetadata     string `gorm:"column:voucher_detail_metadata"`
	LoyaltyAmount             int    `gorm:"column:loyalty_amount"`
	Owner                     string `gorm:"column:owner"`
	// ROGroups                  string `gorm:"column:ro_groups"`
	// RWGroups                  string `gorm:"column:rw_groups"`
}

func (NormalJSONItemPurchase) TableName() string {
	return "purchase"
}

func main() {
	// runtime.GOMAXPROCS(4)
	dsn := "user=postgres password=p4ssw0rd dbname=recharge host=localhost port=5432 sslmode=disable"
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
	// sqlDB, err := db.DB()
	// if err != nil {
	// 	fmt.Println("Error getting database instance:", err)
	// 	return
	// }

	// // Set the maximum number of connections in the idle connection pool.
	// sqlDB.SetMaxIdleConns(1000)

	// // Set the maximum number of open connections to the database.
	// sqlDB.SetMaxOpenConns(10000)

	// // Set the maximum lifetime of a connection.
	// sqlDB.SetConnMaxLifetime(time.Hour)

	// Migrate struktur tabel jika diperlukan
	db.AutoMigrate(&NormalJSONItemPurchase{})

	// Baca file JSON dari DynamoDB
	fileName := []string{
		"./26krcxsxmi2ati6kjsmedtzuoe.json",
		"./65zcohqxcm2a3ovdsmnkdwr6su.json",
		"./any3252eze3vdpvfibtjarnidy.json",
		"./rd5xpyuet454zbqlz7pyamuwtm.json",
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
			buf := make([]byte, 0, 64*1024)
			scanner.Buffer(buf, 1024*1024)
			for scanner.Scan() {
				var dynamoDBItem DynamoDBItemPurchase
				line := scanner.Bytes()
				if err := json.Unmarshal(line, &dynamoDBItem); err != nil {
					fmt.Println("Error decoding JSON:", err)
					return
				}

				// Sekarang Anda memiliki normalJSONItem yang berisi data dari setiap baris JSON
				// Anda dapat melakukan apapun yang Anda butuhkan di sini, misalnya menyimpan ke database
				// Buat objek JSON biasa
				normalJSONItem := NormalJSONItemPurchase{}
				normalJSONItem.ID = handleMissingString(dynamoDBItem.Item.ID.S)
				normalJSONItem.UserID = handleMissingString(dynamoDBItem.Item.UserID.S)
				normalJSONItem.UserPhoneNumber = handleMissingString(dynamoDBItem.Item.UserPhoneNumber.S)
				normalJSONItem.CreatedAt = handleMissingString(dynamoDBItem.Item.CreatedAt.S)
				normalJSONItem.UpdatedAt = handleMissingString(dynamoDBItem.Item.UpdatedAt.S)
				normalJSONItem.CreatedBy = handleMissingString(dynamoDBItem.Item.CreatedBy.S)
				normalJSONItem.UpdatedBy = handleMissingString(dynamoDBItem.Item.UpdatedBy.S)
				normalJSONItem.Amount = handleMissingInt(dynamoDBItem.Item.Amount.N)
				normalJSONItem.DiscountAmount = handleMissingInt(dynamoDBItem.Item.DiscountAmount.N)
				normalJSONItem.PurchaseType = handleMissingString(dynamoDBItem.Item.PurchaseType.S)
				normalJSONItem.Status = handleMissingString(dynamoDBItem.Item.Status.S)
				normalJSONItem.ReferenceNumber = handleMissingString(dynamoDBItem.Item.ReferenceNumber.S)
				normalJSONItem.PaymentGatewayName = handleMissingString(dynamoDBItem.Item.PaymentGatewayName.S)
				normalJSONItem.AdminFee = handleMissingInt(dynamoDBItem.Item.AdminFee.N)
				normalJSONItem.DeviceID = handleMissingString(dynamoDBItem.Item.DeviceID.S)
				normalJSONItem.InvoiceNumber = handleMissingString(dynamoDBItem.Item.InvoiceNumber.S)
				normalJSONItem.PromoName = handleMissingString(dynamoDBItem.Item.PromoName.S)
				normalJSONItem.PromoID = handleMissingString(dynamoDBItem.Item.PromoID.S)
				normalJSONItem.PromoAmount = handleMissingInt(dynamoDBItem.Item.PromoAmount.N)
				normalJSONItem.NormalAmount = handleMissingInt(dynamoDBItem.Item.NormalAmount.N)
				normalJSONItem.ProductID = handleMissingString(dynamoDBItem.Item.ProductID.S)
				normalJSONItem.ProductType = handleMissingString(dynamoDBItem.Item.ProductType.S)
				normalJSONItem.ProductName = handleMissingString(dynamoDBItem.Item.ProductName.S)
				// normalJSONItem.MetaRequestPayment = handleMissingString(dynamoDBItem.Item.MetaRequestPayment.S)
				// normalJSONItem.MetaResponsePayment = handleMissingString(dynamoDBItem.Item.MetaResponsePayment.S)
				// normalJSONItem.MetaPaymentNotification = handleMissingString(dynamoDBItem.Item.MetaPaymentNotification.S)
				// normalJSONItem.ProductMetadata = handleMissingString(dynamoDBItem.Item.ProductMetadata.S)
				normalJSONItem.Tags = handleMissingString(dynamoDBItem.Item.Tags.S)
				// normalJSONItem.CouponMetadata = handleMissingString(dynamoDBItem.Item.CouponMetadata.S)
				// normalJSONItem.PowerbankPackagesMetadata = handleMissingString(dynamoDBItem.Item.PowerbankPackagesMetadata.S)
				// normalJSONItem.UserPbPackageMetadata = handleMissingString(dynamoDBItem.Item.UserPbPackageMetadata.S)
				normalJSONItem.VoucherMasterID = handleMissingString(dynamoDBItem.Item.VoucherMasterID.S)
				normalJSONItem.VoucherDetailID = handleMissingString(dynamoDBItem.Item.VoucherDetailID.S)
				// normalJSONItem.VoucherMasterMetadata = handleMissingString(dynamoDBItem.Item.VoucherMasterMetadata.S)
				// normalJSONItem.VoucherDetailMetadata = handleMissingString(dynamoDBItem.Item.VoucherDetailMetadata.S)
				normalJSONItem.LoyaltyAmount = handleMissingInt(dynamoDBItem.Item.LoyaltyAmount.N)
				normalJSONItem.Owner = handleMissingString(dynamoDBItem.Item.Owner.S)

				// Simpan data ke database
				if err := db.Model(&NormalJSONItemPurchase{}).Create(&normalJSONItem).Error; err != nil {
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
