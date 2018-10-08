package report

import (
	"log"
	"math/rand"
	"time"

	"github.com/TerrexTech/uuuid"
	"github.com/pkg/errors"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

// type ModifyReportData struct {
// 	eth        Ethylene
// 	Datearr    int64
// 	Expirydate int64
// 	Timestamp  int64
// 	Randnum    int64
// }

func random(min, max int64) int64 {
	return rand.Int63n(max-min) + min
}

func generateRandomValue(num1, num2 int64) int64 {
	// rand.Seed(time.Now().Unix())
	return random(num1, num2)
}

func randomFloat(min, max float64) float64 {
	return min + rand.Float64()*(max-min)
}

func genFloatRandomVal(num1, num2 float64) float64 {
	// rand.Seed(time.Now().Unix())
	return randomFloat(num1, num2)
}

func generateNewUUID() uuuid.UUID {
	uuid, err := uuuid.NewV4()
	if err != nil {
		err = errors.Wrap(err, "Unable to generate UUID")
		log.Println(err)
	}
	return uuid
}

var productsName = []string{"Banana", "Orange", "Apple", "Mango", "Strawberry", "Tomato", "Lettuce", "Pear", "Grapes", "Sweet Pepper"}
var locationName = []string{"A101", "B201", "O301", "M401", "S501", "T601", "L701", "P801", "G901", "SW1001"}
var provinceNames = []string{"ON Canada", "BC Canada", "SK Canada", "MN Canada", "NS Canada", "PEI Canada", "QC Canada"}
var reportTypes = []string{"Metric", "Inventory"}

type GeneratedData struct {
	RType Report
	MType Metric
	IType Inventory
}

func GenData() GeneratedData {

	randNameAndLocation := generateRandomValue(1, 10)
	randOrigin := generateRandomValue(1, 6)
	randDateArr := generateRandomValue(1, 6)                          //in hours
	randTimestamp := generateRandomValue(randDateArr, randDateArr+1)  //in hours
	randExpiry := generateRandomValue(((randTimestamp / 24) + 1), 21) //in days
	randDatesold := generateRandomValue(randTimestamp, randExpiry*24) //in hours
	randPrice := generateRandomValue(5000, 10000)
	randTotalWeight := generateRandomValue(100, 300)
	randWasteWeight := generateRandomValue(1, 80)
	randEthylene := genFloatRandomVal(10, 100)
	randTempIn := genFloatRandomVal(20.1, 27.9)
	randHumidity := genFloatRandomVal(60, 90)
	randCarbon := genFloatRandomVal(400, 1800)

	// randProdQuantity := generateRandomValue(100, 300)
	itemId := generateNewUUID()
	sku := GenFakeBarcode("sku")
	deviceId := generateNewUUID()
	location := locationName[randNameAndLocation]
	customerId := generateNewUUID()

	report := Report{
		ItemID:       itemId,
		ReportID:     generateNewUUID(),
		RsCustomerID: customerId,
		ReportType:   reportTypes[generateRandomValue(1, 2)],
		AggregateID:  2,
		Timestamp:    time.Now().Unix(),
	}

	metric := Metric{
		ItemID:      itemId,
		DeviceID:    deviceId,
		AggregateID: 3,
		Ethylene:    randEthylene,
		TempIn:      randTempIn,
		Humidity:    randHumidity,
		CarbonDi:    randCarbon,
		Timestamp:   time.Now().Unix(),
	}

	inventory := Inventory{
		ItemID:       itemId,
		UPC:          GenFakeBarcode("upc"),
		SKU:          sku,
		RsCustomerID: customerId,
		DeviceID:     deviceId,
		Name:         productsName[randNameAndLocation-1], //-1 because rand starts from 1
		Origin:       provinceNames[randOrigin-1],
		TotalWeight:  float64(randTotalWeight),
		Price:        float64(randPrice),
		Location:     location,
		WasteWeight:  float64(randWasteWeight - 1),
		DonateWeight: float64(generateRandomValue(1, 21)),
		AggregateID:  2,
		DateArrived:  time.Now().Add(time.Duration(randDateArr) * time.Hour).Unix(),
		ExpiryDate:   time.Now().AddDate(0, 0, int(randExpiry)).Unix(),
		// Timestamp:    time.Now().Add(time.Duration(randTimestamp) * time.Hour).Unix(),
		Timestamp: time.Now().Unix(),
		// DateSold:     time.Now().Add(time.Duration(randDatesold) * time.Hour).Unix(),
		DateSold: time.Now().Add(time.Duration(randDatesold) * time.Hour).Unix(),

		SalePrice:  float64(generateRandomValue(2, 4)),
		SoldWeight: float64(generateRandomValue(randWasteWeight, randTotalWeight)),
	}

	genDataForDb := GeneratedData{
		RType: report,
		MType: metric,
		IType: inventory,
	}

	return genDataForDb
}

func GenFakeBarcode(barType string) int64 {
	var num int64
	if barType == "upc" {
		num = generateRandomValue(111111111111, 999999999999)
	}
	if barType == "sku" {
		num = generateRandomValue(11111111, 99999999)
	}
	return num
}
