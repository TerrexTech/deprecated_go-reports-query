package report

import (
	"log"

	"github.com/TerrexTech/go-mongoutils/mongo"
	"github.com/pkg/errors"
)

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
	GetReportByDate(search *SearchByDate) (*Report, error)

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

type ReportSearchFieldVal struct {
	SearchField string      `bson:"search_field,omitempty" json:"search_field,omitempty"`
	SearchVal   interface{} `bson:"search_val,omitempty" json:"search_val,omitempty"`
}

func ReportDB(dbConfig DBIConfig) (*DB, error) {
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
					Name: "ethylene",
				},
			},
			IsUnique: true,
			Name:     "ethylene_index",
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
		SchemaStruct: &User{},
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
func (d *DB) CreateReportData() (*Report, error) {
	report := []Report{}
	for i := 0; i < numOfVal; i++ {
		report = append(report, GenDataForEth())
	}

	for _, v := range ethylene {
		insertResult, err := db.collection.InsertOne(v)
		if err != nil {
			err = errors.Wrap(err, "Unable to insert data")
			log.Println(err)
			return nil, err
		}
		log.Println(insertResult)
	}
	return ethylene, nil
}

func (db *DB) GetReportByDate(search *[]SearchByDate) (*Report, error) {
	var findResults []interface{}

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
	return report, nil
}

func (db *DB) SearchByFieldVal(startDate int64, endDate int64) (*Report, error) {
	reportSearch := []SearchByDate{}

	var findResults []interface{}

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
	return report, nil
}

func (d *DB) GenEthylene() (*Report, error) {
	report := []Report{}

	for _, v := range ethylene {
		insertResult, err := db.collection.InsertOne(v)
		if err != nil {
			err = errors.Wrap(err, "Unable to insert data")
			log.Println(err)
			return nil, err
		}
		log.Println(insertResult)
	}
	return ethylene, nil
}

// Login authenticates the provided User.
// An error is returned if Authentication fails.
// func (d *DB) Login(user *User) (*User, error) {
// 	authUser := &User{
// 		Username: user.Username,
// 	}

// 	findResults, err := d.collection.Find(authUser)
// 	if err != nil {
// 		err = errors.Wrap(err, "Login: Error getting user from Database")
// 		return nil, err
// 	}
// 	if len(findResults) == 0 {
// 		log.Println("===========================")
// 		return nil, errors.New("Login: Invalid Credentials")
// 	}

// 	newUser := findResults[0].(*User)
// 	passErr := bcrypt.CompareHashAndPassword([]byte(newUser.Password), []byte(user.Password))
// 	// log.Println("%+v", user)
// 	if passErr != nil {
// 		log.Println("++++++++++++++++++++++++++++")
// 		log.Println(passErr)
// 		return nil, errors.New("Login: Invalid Credentials")
// 	}

// 	return newUser, nil
// }

// Collection returns the currrent MongoDB collection being used for user-auth operations.
func (d *DB) Collection() *mongo.Collection {
	return d.collection
}
