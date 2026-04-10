package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"os"
	"runtime"
	"strconv"
	"sync"
	"time"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// id: ID!
//   status: String
//   # ----------
//   userId: ID!
//   userProfile: UserProfileV2 @connection(fields: ["userId"])
//   userPhoneNumber: String
//   userPbPackageIds: String
//   createdAt: AWSDateTime
//   updatedAt: AWSDateTime
//   createdBy: String
//   updatedBy: String
//   isEmployee: Boolean
//   partnerId: String
//   # ----------
//   type: String
//   lifelineId: String # [DEPRECATED]
//   packageId: String # [DEPRECATED]
//   promoId: String
//   promoCode: String
//   # ----------
//   terminalBorrowId: String
//   terminalReturnId: String
//   borrowTerminalName: String
//   returnTerminalName: String
//   terminalBorrowLocation: String # [DEPRECATED]
//   terminalReturnLocation: String # [DEPRECATED]
//   # ----------
//   pbSerialNumber: String
//   lineType: String
//   borrowTime: AWSDateTime
//   returnTime: AWSDateTime
//   duration: Int # -- in hour
//   terminalPaymentId: String # implemented payment scheme on terminal [DEPRECATED]
//   balancePaymentModelId: String # implemented payment scheme on terminal for balance
//   packagePaymentModelId: String # implemented payment scheme on terminal for package
//   # ----------
//   normalPrice: Int # this is rent fee without promo used [DEPRECATED]
//   promoPrice: Int # [DEPRECATED]
//   rentFee: Int # this is final rent fee if any promo used
//   normalFee: Int # this is rent fee without promo used
//   discountAmount: Int
//   # ----------
//   userCreditBefore: Int # [DEPRECATED]
//   userCreditAfter: Int # [DEPRECATED]
//   # ----------
//   userMetadata: String # record rent user state and return user state, will be implemented in serverless
//   rentLocationMetadata: String
//   returnLocationMetadata: String
//   rentPartnerMetadata: String
//   returnPartnerMetadata: String
//   powerbankMetadata: String
//   feeUsingBalanceMetadata: String
//   salesMetadata: String
//   fieldOfficerMetadata: String
//   trafficMetadata: String
//   isFirstRent: Boolean
//   # ----------
//   tags: String
//   notes: String
//   #if using package, calculate the price from package
//   ratePackageFee: Int
//   rentPackageFee: Int
//   # Migration
//   legacyUserId: String
//   legacyUserPbPackageId: ID
//   legacyUserPbPackage: UserPbPackage
//     @connection(fields: ["legacyUserPbPackageId"])
//   ldTaskId: String
//   # Authorization
//   owner: String # user id [DEPRECATED]
//   roGroups: [String] # machine partner
//   rwGroups: [String]

// Struct untuk menyimpan data dari JSON DynamoDB
type DynamoDBItemPurchase struct {
	Item struct {
		ID                      struct{ S string }
		Status                  struct{ S string }
		UserID                  struct{ S string }
		UserPhoneNumber         struct{ S string }
		UserPbPackageIds        struct{ S string }
		CreatedAt               struct{ S string }
		UpdatedAt               struct{ S string }
		CreatedBy               struct{ S string }
		UpdatedBy               struct{ S string }
		IsEmployee              struct{ BOOL bool }
		PartnerID               struct{ S string }
		Type                    struct{ S string }
		LifelineID              struct{ S string }
		PackageID               struct{ S string }
		PromoID                 struct{ S string }
		PromoCode               struct{ S string }
		TerminalBorrowID        struct{ S string }
		TerminalReturnID        struct{ S string }
		BorrowTerminalName      struct{ S string }
		ReturnTerminalName      struct{ S string }
		TerminalBorrowLocation  struct{ S string }
		TerminalReturnLocation  struct{ S string }
		PbSerialNumber          struct{ S string }
		LineType                struct{ S string }
		BorrowTime              struct{ S string }
		ReturnTime              struct{ S string }
		Duration                struct{ N string }
		TerminalPaymentID       struct{ S string }
		BalancePaymentModelID   struct{ S string }
		PackagePaymentModelID   struct{ S string }
		NormalPrice             struct{ N string }
		PromoPrice              struct{ N string }
		RentFee                 struct{ N string }
		NormalFee               struct{ N string }
		DiscountAmount          struct{ N string }
		UserCreditBefore        struct{ N string }
		UserCreditAfter         struct{ N string }
		UserMetadata            struct{ S string }
		RentLocationMetadata    struct{ S string }
		ReturnLocationMetadata  struct{ S string }
		RentPartnerMetadata     struct{ S string }
		ReturnPartnerMetadata   struct{ S string }
		PowerbankMetadata       struct{ S string }
		FeeUsingBalanceMetadata struct{ S string }
		SalesMetadata           struct{ S string }
		FieldOfficerMetadata    struct{ S string }
		TrafficMetadata         struct{ S string }
		IsFirstRent             struct{ BOOL bool }
		Tags                    struct{ S string }
		Notes                   struct{ S string }
		RatePackageFee          struct{ N string }
		RentPackageFee          struct{ N string }
		LegacyUserID            struct{ S string }
		LegacyUserPbPackageID   struct{ S string }
		LdTaskID                struct{ S string }
		Owner                   struct{ S string }
	}
}

// Struct untuk menyimpan data dalam bentuk JSON biasa
type NormalJSONItemPurchase struct {
	ID                      string `gorm:"column:id"`
	Status                  string `gorm:"column:status"`
	UserID                  string `gorm:"column:user_id"`
	UserPhoneNumber         string `gorm:"column:user_phone_number"`
	UserPbPackageIds        string `gorm:"column:user_pb_package_ids"`
	CreatedAt               string `gorm:"column:created_at"`
	UpdatedAt               string `gorm:"column:updated_at"`
	CreatedBy               string `gorm:"column:created_by"`
	UpdatedBy               string `gorm:"column:updated_by"`
	IsEmployee              bool   `gorm:"column:is_employee"`
	PartnerID               string `gorm:"column:partner_id"`
	Type                    string `gorm:"column:type"`
	LifelineID              string `gorm:"column:lifeline_id"`
	PackageID               string `gorm:"column:package_id"`
	PromoID                 string `gorm:"column:promo_id"`
	PromoCode               string `gorm:"column:promo_code"`
	TerminalBorrowID        string `gorm:"column:terminal_borrow_id"`
	TerminalReturnID        string `gorm:"column:terminal_return_id"`
	BorrowTerminalName      string `gorm:"column:borrow_terminal_name"`
	ReturnTerminalName      string `gorm:"column:return_terminal_name"`
	TerminalBorrowLocation  string `gorm:"column:terminal_borrow_location"`
	TerminalReturnLocation  string `gorm:"column:terminal_return_location"`
	PbSerialNumber          string `gorm:"column:pb_serial_number"`
	LineType                string `gorm:"column:line_type"`
	BorrowTime              string `gorm:"column:borrow_time"`
	ReturnTime              string `gorm:"column:return_time"`
	Duration                int    `gorm:"column:duration"`
	TerminalPaymentID       string `gorm:"column:terminal_payment_id"`
	BalancePaymentModelID   string `gorm:"column:balance_payment_model_id"`
	PackagePaymentModelID   string `gorm:"column:package_payment_model_id"`
	NormalPrice             int    `gorm:"column:normal_price"`
	PromoPrice              int    `gorm:"column:promo_price"`
	RentFee                 int    `gorm:"column:rent_fee"`
	NormalFee               int    `gorm:"column:normal_fee"`
	DiscountAmount          int    `gorm:"column:discount_amount"`
	UserCreditBefore        int    `gorm:"column:user_credit_before"`
	UserCreditAfter         int    `gorm:"column:user_credit_after"`
	UserMetadata            string `gorm:"column:user_metadata"`
	RentLocationMetadata    string `gorm:"column:rent_location_metadata"`
	ReturnLocationMetadata  string `gorm:"column:return_location_metadata"`
	RentPartnerMetadata     string `gorm:"column:rent_partner_metadata"`
	ReturnPartnerMetadata   string `gorm:"column:return_partner_metadata"`
	PowerbankMetadata       string `gorm:"column:powerbank_metadata"`
	FeeUsingBalanceMetadata string `gorm:"column:fee_using_balance_metadata"`
	SalesMetadata           string `gorm:"column:sales_metadata"`
	FieldOfficerMetadata    string `gorm:"column:field_officer_metadata"`
	TrafficMetadata         string `gorm:"column:traffic_metadata"`
	IsFirstRent             bool   `gorm:"column:is_first_rent"`
	Tags                    string `gorm:"column:tags"`
	Notes                   string `gorm:"column:notes"`
	RatePackageFee          int    `gorm:"column:rate_package_fee"`
	RentPackageFee          int    `gorm:"column:rent_package_fee"`
	LegacyUserID            string `gorm:"column:legacy_user_id"`
	LegacyUserPbPackageID   string `gorm:"column:legacy_user_pb_package_id"`
	LdTaskID                string `gorm:"column:ld_task_id"`
	Owner                   string `gorm:"column:owner"`
	// ROGroups                  string `gorm:"column:ro_groups"`
	// RWGroups                  string `gorm:"column:rw_groups"`
}

func (NormalJSONItemPurchase) TableName() string {
	return "transaction_rent3"
}

var (
	dbConnString   = "user=postgres password=p4ssw0rd dbname=recharge host=localhost port=5432 sslmode=disable"
	totalWorker    = 500
	dbMaxConns     = 500
	dbMaxIdleConns = 20
)

func main() {
	runtime.GOMAXPROCS(6)
	start := time.Now()
	// db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	// if err != nil {
	// 	fmt.Println("Error connecting to the database:", err)
	// 	return
	// }

	db, err := gorm.Open(postgres.Open(dbConnString), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Error), // Set logger level (optional)
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
	sqlDB.SetMaxIdleConns(dbMaxIdleConns)

	// Set the maximum number of open connections to the database.
	sqlDB.SetMaxOpenConns(dbMaxConns)

	// Set the maximum lifetime of a connection.
	sqlDB.SetConnMaxLifetime(time.Minute)

	// Migrate struktur tabel jika diperlukan``
	db.AutoMigrate(&NormalJSONItemPurchase{})

	// Baca file JSON dari DynamoDB
	fileName := []string{
		"./7wsf2vcbdm7pxezlifeq2fxtrm.json",
		"./65snvlwqhmzzta253ysvp2q7li.json",
		"./auoxom4n3a53dcjx37yo3ivel4.json",
		"./b7kolybjvm3bzkhmilpbkwle3e.json",
		"./bj6tw7c4yi4wvgcwhyyy2foyie.json",
		"./coqelxo6di6vhbdx32dmqqbe24.json",
		"./ei22f23zhq7njazql7hez3f4ui.json",
		"./gce2kqgaaa7ejk5dty4wawx564.json",
		"./irkqzz3kxq4g3emwqvdtnaxrly.json",
		"./kkihw353pu73zmozxym2m4rzji.json",
		"./l6rjh3wlpu7b3mqxu673qo2zwy.json",
		"./muwbij7qce2svojnvdnbdhrsdq.json",
		"./n4amgsf2sq6qxd32owr5k57eda.json",
		"./n5cafzhtee3orh2uxh47soxhmy.json",
		"./sucvxlepkq26pgopwnspvxl2mm.json",
		"./t2nf7a2yl43rzhjkhzp4mriayy.json",
	}
	// wg.Add(len(fileName))
	for _, v := range fileName {
		skener, jsonFile, err := openJsonFile(v)
		if err != nil {
			log.Fatal(err.Error())
		}
		defer jsonFile.Close()

		jobs := make(chan DynamoDBItemPurchase)
		wg := new(sync.WaitGroup)

		go dispatchWorkers(db, jobs, wg)
		readCsvFilePerLineThenSendToWorker(skener, jobs, wg)
		if err := skener.Err(); err != nil {
			fmt.Println("Error reading file:", err)
			return
		}

		wg.Wait()

		duration := time.Since(start)
		fmt.Println("1 file done in", int(math.Ceil(duration.Seconds())), "seconds")
	}
	fmt.Println("All goroutines have finished.")
}

func openJsonFile(fileName string) (*bufio.Scanner, *os.File, error) {
	log.Println("=> open csv file")

	f, err := os.Open(fileName)
	if err != nil {
		if os.IsNotExist(err) {
			log.Fatal("file tidak ditemukan.")
		}

		return nil, nil, err
	}

	scanner := bufio.NewScanner(f)
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024)
	return scanner, f, nil
}

func readCsvFilePerLineThenSendToWorker(skener *bufio.Scanner, jobs chan<- DynamoDBItemPurchase, wg *sync.WaitGroup) {
	for skener.Scan() {
		line := skener.Bytes()
		var item DynamoDBItemPurchase
		if err := json.Unmarshal(line, &item); err != nil {
			fmt.Println("Error decoding JSON:", err)
			return
		}

		wg.Add(1)
		jobs <- item
	}
	close(jobs)
}

func dispatchWorkers(db *gorm.DB, jobs <-chan DynamoDBItemPurchase, wg *sync.WaitGroup) {
	for workerIndex := 0; workerIndex <= totalWorker; workerIndex++ {
		go func(workerIndex int, db *gorm.DB, jobs <-chan DynamoDBItemPurchase, wg *sync.WaitGroup) {
			counter := 0

			for job := range jobs {
				doTheJob(workerIndex, counter, db, job)
				wg.Done()
				counter++
			}
		}(workerIndex, db, jobs, wg)
	}
}

func doTheJob(workerIndex, counter int, db *gorm.DB, dynamoDBItem DynamoDBItemPurchase) {
	for {
		var outerError error
		func(outerError *error) {
			defer func() {
				if err := recover(); err != nil {
					*outerError = fmt.Errorf("%v", err)
				}
			}()

			{
				normalJSONItem := NormalJSONItemPurchase{}
				normalJSONItem.ID = handleMissingString(dynamoDBItem.Item.ID.S)
				normalJSONItem.Status = handleMissingString(dynamoDBItem.Item.Status.S)
				normalJSONItem.UserID = handleMissingString(dynamoDBItem.Item.UserID.S)
				normalJSONItem.UserPhoneNumber = handleMissingString(dynamoDBItem.Item.UserPhoneNumber.S)
				normalJSONItem.UserPbPackageIds = handleMissingString(dynamoDBItem.Item.UserPbPackageIds.S)
				normalJSONItem.CreatedAt = handleMissingString(dynamoDBItem.Item.CreatedAt.S)
				normalJSONItem.UpdatedAt = handleMissingString(dynamoDBItem.Item.UpdatedAt.S)
				normalJSONItem.CreatedBy = handleMissingString(dynamoDBItem.Item.CreatedBy.S)
				normalJSONItem.UpdatedBy = handleMissingString(dynamoDBItem.Item.UpdatedBy.S)
				normalJSONItem.IsEmployee = dynamoDBItem.Item.IsEmployee.BOOL
				normalJSONItem.PartnerID = handleMissingString(dynamoDBItem.Item.PartnerID.S)
				normalJSONItem.Type = handleMissingString(dynamoDBItem.Item.Type.S)
				normalJSONItem.LifelineID = handleMissingString(dynamoDBItem.Item.LifelineID.S)
				normalJSONItem.PackageID = handleMissingString(dynamoDBItem.Item.PackageID.S)
				normalJSONItem.PromoID = handleMissingString(dynamoDBItem.Item.PromoID.S)
				normalJSONItem.PromoCode = handleMissingString(dynamoDBItem.Item.PromoCode.S)
				normalJSONItem.TerminalBorrowID = handleMissingString(dynamoDBItem.Item.TerminalBorrowID.S)
				normalJSONItem.TerminalReturnID = handleMissingString(dynamoDBItem.Item.TerminalReturnID.S)
				normalJSONItem.BorrowTerminalName = handleMissingString(dynamoDBItem.Item.BorrowTerminalName.S)
				normalJSONItem.ReturnTerminalName = handleMissingString(dynamoDBItem.Item.ReturnTerminalName.S)
				normalJSONItem.TerminalBorrowLocation = handleMissingString(dynamoDBItem.Item.TerminalBorrowLocation.S)
				normalJSONItem.TerminalReturnLocation = handleMissingString(dynamoDBItem.Item.TerminalReturnLocation.S)
				normalJSONItem.PbSerialNumber = handleMissingString(dynamoDBItem.Item.PbSerialNumber.S)
				normalJSONItem.LineType = handleMissingString(dynamoDBItem.Item.LineType.S)
				normalJSONItem.BorrowTime = handleMissingString(dynamoDBItem.Item.BorrowTime.S)
				normalJSONItem.ReturnTime = handleMissingString(dynamoDBItem.Item.ReturnTime.S)
				normalJSONItem.Duration = handleMissingInt(dynamoDBItem.Item.Duration.N)
				normalJSONItem.TerminalPaymentID = handleMissingString(dynamoDBItem.Item.TerminalPaymentID.S)
				normalJSONItem.BalancePaymentModelID = handleMissingString(dynamoDBItem.Item.BalancePaymentModelID.S)
				normalJSONItem.PackagePaymentModelID = handleMissingString(dynamoDBItem.Item.PackagePaymentModelID.S)
				normalJSONItem.NormalPrice = handleMissingInt(dynamoDBItem.Item.NormalPrice.N)
				normalJSONItem.PromoPrice = handleMissingInt(dynamoDBItem.Item.PromoPrice.N)
				normalJSONItem.RentFee = handleMissingInt(dynamoDBItem.Item.RentFee.N)
				normalJSONItem.NormalFee = handleMissingInt(dynamoDBItem.Item.NormalFee.N)
				normalJSONItem.DiscountAmount = handleMissingInt(dynamoDBItem.Item.DiscountAmount.N)
				normalJSONItem.UserCreditBefore = handleMissingInt(dynamoDBItem.Item.UserCreditBefore.N)
				normalJSONItem.UserCreditAfter = handleMissingInt(dynamoDBItem.Item.UserCreditAfter.N)
				normalJSONItem.UserMetadata = handleMissingString(dynamoDBItem.Item.UserMetadata.S)
				normalJSONItem.RentLocationMetadata = handleMissingString(dynamoDBItem.Item.RentLocationMetadata.S)
				normalJSONItem.ReturnLocationMetadata = handleMissingString(dynamoDBItem.Item.ReturnLocationMetadata.S)
				normalJSONItem.RentPartnerMetadata = handleMissingString(dynamoDBItem.Item.RentPartnerMetadata.S)
				normalJSONItem.ReturnPartnerMetadata = handleMissingString(dynamoDBItem.Item.ReturnPartnerMetadata.S)
				normalJSONItem.PowerbankMetadata = handleMissingString(dynamoDBItem.Item.PowerbankMetadata.S)
				normalJSONItem.FeeUsingBalanceMetadata = handleMissingString(dynamoDBItem.Item.FeeUsingBalanceMetadata.S)
				normalJSONItem.SalesMetadata = handleMissingString(dynamoDBItem.Item.SalesMetadata.S)
				normalJSONItem.FieldOfficerMetadata = handleMissingString(dynamoDBItem.Item.FieldOfficerMetadata.S)
				normalJSONItem.TrafficMetadata = handleMissingString(dynamoDBItem.Item.TrafficMetadata.S)
				normalJSONItem.IsFirstRent = dynamoDBItem.Item.IsFirstRent.BOOL
				normalJSONItem.Tags = handleMissingString(dynamoDBItem.Item.Tags.S)
				normalJSONItem.Notes = handleMissingString(dynamoDBItem.Item.Notes.S)
				normalJSONItem.RatePackageFee = handleMissingInt(dynamoDBItem.Item.RatePackageFee.N)
				normalJSONItem.RentPackageFee = handleMissingInt(dynamoDBItem.Item.RentPackageFee.N)
				normalJSONItem.LegacyUserID = handleMissingString(dynamoDBItem.Item.LegacyUserID.S)
				normalJSONItem.LegacyUserPbPackageID = handleMissingString(dynamoDBItem.Item.LegacyUserPbPackageID.S)
				normalJSONItem.LdTaskID = handleMissingString(dynamoDBItem.Item.LdTaskID.S)
				normalJSONItem.Owner = handleMissingString(dynamoDBItem.Item.Owner.S)
				if err := db.Model(&NormalJSONItemPurchase{}).Create(&normalJSONItem).Error; err != nil {
					fmt.Println("Error inserting data into database:", err)
					return
				}
			}
		}(&outerError)
		if outerError == nil {
			break
		}
	}

	if counter%200 == 0 {
		log.Println("=> worker", workerIndex, "inserted", counter, "data")
	}
}

func handleMissingString(value string) string {
	if value == "" {
		return ""
	}
	return value
}

// Helper function to handle missing number values
// func handleMissingNumber(value string) string {
// 	if value == "" {
// 		return "0"
// 	}
// 	return value
// }

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
// func handleMissingStringArray(value []string) string {
// 	if len(value) == 0 {
// 		return ""
// 	}
// 	return value[0]
// }
