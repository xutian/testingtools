package main

import (
	"os"
	"time"
	"runtime"
	"context"
	"database/sql"
	"flag"
	"fmt"
	"sync"
	"log"
	_ "github.com/go-sql-driver/mysql"
)


var db *sql.DB

var (
	host string
	dbName string
	user string
	passwd string
	port int
	timeout int64
	help bool
)



func init() {
	flag.BoolVar(&help,"help", false, "Print this help")
	flag.IntVar(&port, "port", 3331, "mysql database port, default is 3331")

	flag.StringVar(&user, "user","admin", "User for database connection, default is admin")
	flag.StringVar(&passwd, "passwd", "Tx123456", "Password for database connection, default is 'Tx123456'")
	flag.StringVar(&host, "host", "127.0.0.1", "Host to connect to database, default is 127.0.0.1")
	flag.StringVar(&dbName, "db", "mysql", "Database to connect, default is mysql")
	flag.Int64Var(&timeout, "timeout", 5000, "Testing timeout, default is 5000")
}

func initDB()(err error) {

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/mysql?charset=utf8mb4&parseTime=True", user, passwd,host, port)
	log.Printf("Connecting to database with: %s", dsn)
    db, err = sql.Open("mysql", dsn)
    if err != nil {
		return err
    }

    err = db.Ping()
    if err != nil {
        return err
    }
    db.SetMaxIdleConns(10)
    db.SetMaxOpenConns(200)
    db.SetConnMaxLifetime(time.Second * 10)
	return nil
}

func createDatabase(db_name string) (err error) {
	sql_db := fmt.Sprintf("create database if not exists %s;", db_name)
	log.Printf("Create database sql: %s\n", sql_db)
	if _, err := db.Exec(sql_db); err != nil {
		fmt.Printf("Create database %s failed: %v \n", db_name, err)
		return err
	}
	tab_name := fmt.Sprintf("%s.t1", db_name)
	sql_tab := fmt.Sprintf("Create table if not exists %s (rid int(11) not null auto_increment, rcount int not null, rtime datetime not null default now(), primary key(rid));", tab_name)
	log.Printf("Create table sql: %s\n", sql_tab)
	if _, err := db.Exec(sql_tab); err != nil {
		fmt.Printf("Create test table %s.t1 failed: %v \n", db_name, err)
		return err
	}
	return nil
}

func createTestDB() []string {
	out := make([]string, 0)
	var wg sync.WaitGroup
	for i:=0; i < runtime.NumCPU(); i++ {
		wg.Add(1)
		go func(i int, dbs * []string){
			defer wg.Done()
			db_name := fmt.Sprintf("%s%d", dbName, i)
			tab_name := fmt.Sprintf("%s.t1", db_name)
			log.Printf("Create database %s, table %s", db_name, tab_name)
			if err := createDatabase(db_name); err != nil {
				return
			}
			*dbs = append(*dbs, tab_name)
		}(i, &out)
	}
	wg.Wait()
	return out
}


func insertData(ctx context.Context, dbs []string) {
	var wg sync.WaitGroup
	for _, db_name := range dbs{
		wg.Add(1)
		// write data to each database in a go routine
		go func (db_name string) {
			defer wg.Done()
			sql := fmt.Sprintf("insert into %s (rcount) values (?);", db_name)
			//loop, go will exit if timeout
			for i :=0;;i++{
				select {
				case <- ctx.Done():
					fmt.Printf("Time's up, go will exit")
					return 
				default:
					ret, err := db.Exec(sql, i)
					if err != nil {
						fmt.Printf("insert failed, err:%v\n", err)
					} 
					the_id, err := ret.LastInsertId()
					if err != nil {
						fmt.Printf("get lastinsert ID failed, err:%v\n", err)
					}else{
						fmt.Printf("insert success, the id is %d.\n", the_id)
					}
				}
			}
		}(db_name)

	}
	wg.Wait()
}



func main(){

	flag.Parse()
	if help {
		flag.Usage()
		os.Exit(0)
	}

	if err := initDB(); err != nil {
		log.Fatalf("Content database failed, err: %v\n", err)
	}
	defer db.Close()
	log.Printf("Starting insert database, will finish after %d seconds.\n", timeout)
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout * int64(time.Second)))
	defer cancel()
	db_lst := createTestDB()
	insertData(ctx, db_lst)
	log.Println("Done")
	os.Exit(0)
}