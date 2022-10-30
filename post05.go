package post05

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"

	_ "github.com/lib/pq"
)

// Connection details
var (
	Hostname = ""
	Port     = 2345
	Username = ""
	Password = ""
	Database = ""
)

type MSDSCourse struct {
	CID 	   string 
	CNAME      string
	CPREREQ    string
}

func openConnection() (*sql.DB, error) {
	// connection string
	conn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		Hostname, Port, Username, Password, Database)

	// open database
	db, err := sql.Open("postgres", conn)
	if err != nil {
		return nil, err
	}
	return db, nil
}

// The function returns the User ID of the username
// -1 if the user does not exist
func exists(cname string) string {
	cname = strings.ToLower(cname)

	db, err := openConnection()
	if err != nil {
		fmt.Println(err)
		return "no"
	}
	defer db.Close()

	courseID := "no"
	statement := fmt.Sprintf(`SELECT "cid" FROM "MSDSCourse" where cname = '%s'`, cname)
	rows, err := db.Query(statement)

	for rows.Next() {
		var cid string
		err = rows.Scan(&cid)
		if err != nil {
			fmt.Println("Scan", err)
			return "no"
		}
		courseID = cid
	}
	defer rows.Close()
	return courseID
}

// AddUser adds a new user to the database
// Returns new User ID
// -1 if there was an error
func AddCourse(d MSDSCourse) string {
	d.CNAME = strings.ToLower(d.CNAME)

	db, err := openConnection()
	if err != nil {
		fmt.Println(err)
		return "no"
	}
	defer db.Close()

	courseID := exists(d.CNAME)
	if courseID != "no" {
		fmt.Println("Course already exists:", d.CNAME)
		return "no"
	}

	insertStatement := `insert into "MSDSCourse" ("CNAME") values ($1)`
	_, err = db.Exec(insertStatement, d.CNAME)
	if err != nil {
		fmt.Println(err)
		return "no"
	}

	courseID = exists(d.CNAME)
	if courseID == "no" {
		return courseID
	}

	insertStatement = `insert into "MSDSCourseCatalog" ("courseid", "cname", "cprereq")
	values ($1, $2, $3, $4)`
	_, err = db.Exec(insertStatement, courseID, d.CNAME, d.CPREREQ)
	if err != nil {
		fmt.Println("db.Exec()", err)
		return "no"
	}

	return courseID
}

// DeleteUser deletes an existing user
func DeleteCourse(cid string) error {
	db, err := openConnection()
	if err != nil {
		return err
	}
	defer db.Close()

	// Does the ID exist?
	statement := fmt.Sprintf(`SELECT "cname" FROM "MSDSCourse" where cid = %d`, cid)
	rows, err := db.Query(statement)

	var cname string
	for rows.Next() {
		err = rows.Scan(&cname)
		if err != nil {
			return err
		}
	}
	defer rows.Close()

	if exists(cname) != cid {
		return fmt.Errorf("Course with CID %d does not exist", cid)
	}

	// Delete from Userdata
	deleteStatement := `delete from "MSDSCourseCatalog" where cid=$1`
	_, err = db.Exec(deleteStatement, cid)
	if err != nil {
		return err
	}

	// Delete from Users
	deleteStatement = `delete from "MSDSCourse" where cid=$1`
	_, err = db.Exec(deleteStatement, cid)
	if err != nil {
		return err
	}

	return nil
}

// ListUsers lists all users in the database
func ListCourses() ([]MSDSCourse, error) {
	Data := []MSDSCourse{}
	db, err := openConnection()
	if err != nil {
		return Data, err
	}
	defer db.Close()

	rows, err := db.Query(`SELECT "cid","cname","cprereq")
		FROM "MSDSCourse","MSDSCourseCatalog"
		WHERE MSDSCourse.id = MSDSCourseCatalog.userid`)
	if err != nil {
		return Data, err
	}

	for rows.Next() {
		var cid string
		var cname string
		var cprereq string
		err = rows.Scan(&cid, &cname, &cprereq)
		temp := MSDSCourse{CID: cid, CNAME: cname, CPREREQ: cprereq}
		Data = append(Data, temp)
		if err != nil {
			return Data, err
		}
	}
	defer rows.Close()
	return Data, nil
}

// UpdateUser is for updating an existing user
func UpdateCourse(d MSDSCourse) error {
	db, err := openConnection()
	if err != nil {
		return err
	}
	defer db.Close()

	courseID := exists(d.CNAME)
	if courseID == "no" {
		return errors.New("Course does not exist")
	}
	d.CID = courseID
	updateStatement := `update "MSDSCourseCatalog" set "cname"=$1, "cprereq"=$2 where "courseid"=$4`
	_, err = db.Exec(updateStatement, d.CNAME, d.CPREREQ, d.CID)
	if err != nil {
		return err
	}

	return nil
}
