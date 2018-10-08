package report

import (
	"log"

	"github.com/TerrexTech/go-mongoutils/mongo"
	"github.com/pkg/errors"
)

// type Collections struct {
// 	Report    string
// 	Metric    string
// 	Inventory string
// }

type ConfigSchema struct {
	Report    *Report
	Metric    *Metric
	Inventory *Metric
}

// DBIConfig is the configuration for the authDB.
type DBIConfig struct {
	Hosts               []string
	Username            string
	Password            string
	TimeoutMilliseconds uint32
	Database            string
	Collection          string
}

// DBI is the Database-interface for reporting.
// This fetches/writes data to/from database for generating reports
type DBI interface {
	Collection() *mongo.Collection
	CreateReportData() (*Report, error)
	SearchByTimestamp(search *SearchByDate) (*Report, error)
	SearchByFieldVal(search *[]SearchByFieldVal) (*Report, error)

	// UserByUUID(uid uuuid.UUID) (*User, error)
	// Login(user *User) (*User, error)
}

// DB is the implementation for dbI.
// dbI is the Database-interface for generating reports.
type DB struct {
	collection *mongo.Collection
}

type SearchByDate struct {
	EndDate   int64 `bson:"end_date,omitempty" json:"end_date,omitempty"`
	StartDate int64 `bson:"start_date,omitempty" json:"start_date,omitempty"`
}

type SearchByFieldVal struct {
	SearchField string      `bson:"search_field,omitempty" json:"search_field,omitempty"`
	SearchVal   interface{} `bson:"search_val,omitempty" json:"search_val,omitempty"`
}

func GenerateDB(dbConfig DBIConfig, schema *ConfigSchema) (*DB, error) {
	config := mongo.ClientConfig{
		Hosts:               dbConfig.Hosts,
		Username:            dbConfig.Username,
		Password:            dbConfig.Password,
		TimeoutMilliseconds: dbConfig.TimeoutMilliseconds,
	}

	client, err := mongo.NewClient(config)
	if err != nil {
		err = errors.Wrap(err, "Error creating DB-client")
		return nil, err
	}

	conn := &mongo.ConnectionConfig{
		Client:  client,
		Timeout: 5000,
	}

	indexConfigs := []mongo.IndexConfig{
		mongo.IndexConfig{
			ColumnConfig: []mongo.IndexColumnConfig{
				mongo.IndexColumnConfig{
					Name: "item_id",
				},
			},
			IsUnique: true,
			Name:     "item_id_index",
		},
		mongo.IndexConfig{
			ColumnConfig: []mongo.IndexColumnConfig{
				mongo.IndexColumnConfig{
					Name:        "timestamp",
					IsDescOrder: true,
				},
			},
			IsUnique: true,
			Name:     "timestamp_index",
		},
	}

	// ====> Create New Collection
	collConfig := &mongo.Collection{
		Connection:   conn,
		Database:     dbConfig.Database,
		Name:         dbConfig.Collection,
		SchemaStruct: schema,
		Indexes:      indexConfigs,
	}
	c, err := mongo.EnsureCollection(collConfig)
	if err != nil {
		err = errors.Wrap(err, "Error creating DB-client")
		return nil, err
	}
	return &DB{
		collection: c,
	}, nil
}

// UserByUUID gets the User from DB using specified UUID.
// An error is returned if no user is found.
func (db *DB) CreateReportData(numOfVal int) (*[]Report, error) {
	report := []Report{}
	for i := 0; i < numOfVal; i++ {
		generatedData := GenData()
		report = append(report, generatedData.RType)
	}

	for _, v := range report {
		insertResult, err := db.collection.InsertOne(v)
		if err != nil {
			err = errors.Wrap(err, "Unable to insert data")
			log.Println(err)
			return nil, err
		}
		log.Println(insertResult)
	}
	return &report, nil
}

func (db *DB) SearchByTimestamp(search []SearchByDate) (*[]Report, error) {
	var findResults []interface{}
	var err error
	for _, val := range search {
		if val.StartDate != 0 && val.EndDate != 0 {
			//Find
			findResults, err = db.collection.Find(map[string]interface{}{
				"timestamp": map[string]int64{
					"$lte": val.EndDate,
					"$gte": val.StartDate,
				},
			})
		}

		if val.StartDate == 0 && val.EndDate != 0 {
			findResults, err = db.collection.Find(map[string]interface{}{
				"timestamp": map[string]int64{
					"$lte": val.EndDate,
				},
			})
		}
	}

	if err != nil {
		err = errors.Wrap(err, "Error while fetching product.")
		log.Println(err)
		return nil, err
	}

	//length
	if len(findResults) == 0 {
		msg := "No results found - SearchByDate"
		return nil, errors.New(msg)
	}

	report := []Report{}

	for _, v := range findResults {
		result := v.(*Report)
		report = append(report, *result)
	}
	return &report, nil
}

func (db *DB) SearchByFieldVal(search []SearchByFieldVal) (*[]Report, error) {

	var findResults []interface{}
	var err error

	for _, v := range search {
		if v.SearchField != "" && v.SearchVal != "" {
			findResults, err = db.collection.Find(map[string]interface{}{
				v.SearchField: map[string]interface{}{
					"$eq": &v.SearchVal,
				},
			})
		}
	}

	if err != nil {
		err = errors.Wrap(err, "Error while fetching product.")
		log.Println(err)
		return nil, err
	}

	//length
	if len(findResults) == 0 {
		msg := "No results found - SearchByDate"
		return nil, errors.New(msg)
	}

	report := []Report{}

	for _, v := range findResults {
		result := v.(*Report)
		report = append(report, *result)
	}
	return &report, nil
}

// func (db *DB) InsertIntoReport(report Report) (*mgo.InsertOneResult, error) {
// 	uuid, err := uuuid.NewV4()
// 	if err != nil {
// 		err = errors.Wrap(err, "Unable to generate UUID")
// 		log.Println(err)
// 	}

// 	genReport := report{
// 		ReportID: uuid,
// 		TempIn:   randTempIn, //from load_report
// 		Humidity: randHumidity,
// 		Ethylene: randEthylene,
// 		CarbonDi: randCarbon,
// 	}

// 	insertResult, err := db.collection.InsertOne(genReport)
// 	if err != nil {
// 		err = errors.Wrap(err, "Unable to insert into Report")
// 		log.Println(err)
// 		return nil, err
// 	}

// 	return insertResult, nil

// }

// func (db *DB) EthyleneReport(inventory []Inventory) (*Report, error) {

// 	findResults, err = db.collection.Find(map[string]interface{}{
// 		inventory.SKU: map[string]interface{}{
// 			"$eq": &v.SearchVal,
// 		},
// 	})
// }

// Collection returns the currrent MongoDB collection being used for user-auth operations.
func (d *DB) Collection() *mongo.Collection {
	return d.collection
}
