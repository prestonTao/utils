package utils

import (
	"fmt"
	"os"
	"time"

	// "math/big"
	// "fmt"
	"testing"
)

func TestLevelDBLink(t *testing.T) {
	dbpath := "D:/test/test/leveldbdata"
	os.RemoveAll(dbpath)
	preKey := []byte("nihao")
	// order := 5

	leveldb, _ := CreateLevelDB(dbpath)
	leveldb.AddToStart(&preKey, 5, nil)
	leveldb.AddToStart(&preKey, 4, nil)
	// leveldb.PrintAll()

	total := 10000

	//顺序添加1
	start := time.Now()
	for i := 0; i < total; i++ {
		// orderId := GetRandomOneInt64()
		leveldb.AddToBody(&preKey, uint64(i), nil)
	}
	fmt.Println("顺序添加1耗时:", time.Now().Sub(start))

	// //顺序添加2
	// start = time.Now()
	// for i := 0; i < total; i++ {
	// 	// orderId := GetRandomOneInt64()
	// 	leveldb.AddToEnd(&preKey, uint64(i), nil)
	// }
	// fmt.Println("顺序添加2耗时:", time.Now().Sub(start))

	//随机添加
	// start = time.Now()
	// for i := 0; i < total; i++ {
	// 	orderId := GetRandomOneInt64()
	// 	leveldb.AddToBody(&preKey, uint64(orderId), nil)
	// }
	// fmt.Println("随机添加耗时:", time.Now().Sub(start))
	// leveldb.PrintAll()
}
