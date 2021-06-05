package klinedb

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"
)

func TestBasic(t *testing.T) {
	Clear()
	var timestamp = OneMinute.GetIntervalOffsetTime(int64(1262275200000*time.Millisecond), 1)
	var item = Kline{
		Flag:      1,
		Timestamp: timestamp,
		DealCount: 9999998,
		Open:      rand.Float64(),
		Close:     rand.Float64(),
		High:      rand.Float64(),
		Low:       rand.Float64(),
		Vol:       rand.Float64(),
		QuoteVol:  rand.Float64(),
	}
	var list []Kline
	var _, err = WriteKLines("BTCUSDT", OneMinute, item)
	if err != nil {
		t.Error("case 1 write data error:", err)
		t.Fail()
	}
	list, err = QueryLines("BTCUSDT", OneMinute, timestamp, timestamp)
	if err == nil && len(list) > 0 && item == list[0] {
		t.Logf("case 1 queried data time：%d,DealCount：%d", list[0].Timestamp, list[0].DealCount)
	} else {
		t.Error("case 1 queried data error:", err)
		t.Fail()
	}
	var item1 = Kline{
		Flag:      1,
		Timestamp: OneMinute.GetIntervalOffsetTime(int64(1262275200000*time.Millisecond), 100),
		DealCount: 1335345436,
		Open:      rand.Float64(),
		Close:     rand.Float64(),
		High:      rand.Float64(),
		Low:       rand.Float64(),
		Vol:       rand.Float64(),
		QuoteVol:  rand.Float64(),
	}
	var item2 = Kline{
		Flag:      1,
		Timestamp: OneMinute.GetIntervalOffsetTime(int64(1262275200000*time.Millisecond), 105),
		DealCount: 55555555,
		Open:      rand.Float64(),
		Close:     rand.Float64(),
		High:      rand.Float64(),
		Low:       rand.Float64(),
		Vol:       rand.Float64(),
		QuoteVol:  rand.Float64(),
	}
	_, err = WriteKLines("BTCUSDT", OneMinute, item1, item2)
	if err != nil {
		t.Error("case 2 write data error:", err)
	}
	list, err = QueryLines("BTCUSDT", OneMinute, OneMinute.GetIntervalOffsetTime(int64(1262275200000*time.Millisecond), 100), OneMinute.GetIntervalOffsetTime(int64(1262275200000*time.Millisecond), 105))
	if err == nil && len(list) == 2 && item1 == list[0] && item2 == list[1] {
		t.Logf("case 1 queried")
		t.Logf("Item1:time：%d,DealCount：%d", list[0].Timestamp, list[0].DealCount)
		t.Logf("Item2:time：%d,DealCount：%d", list[1].Timestamp, list[1].DealCount)

	} else {
		t.Error("case 1 queried data error:", err)
		t.Fail()
	}
}

func TestBigData(t *testing.T) {
	Clear()
	InitMapping()
	fmt.Println("Batch write 10-year minute line data:")
	BigDataWrite()
	fmt.Println("Single-thread query of 5-year minute-line data:")
	BigDataQuery()
	fmt.Println("100 concurrent queries for 1 month's minute-line data:")
	ConcurrentQuery()
}

func Clear() {
	var fileInfoList, _ = ioutil.ReadDir(WorkDir)
	for _, info := range fileInfoList {
		os.Remove(WorkDir + info.Name())
	}
}

func BigDataQuery() {
	var time1 = time.Now().UnixNano()
	//查询2010年至2015年的
	var beginTime int64 = int64(1262275200000 * time.Millisecond)
	var endTime int64 = int64(1420041600000 * time.Millisecond)
	var interval = OneMinute
	var data, err = QueryLines("BTCUSDT", interval, beginTime, endTime)
	if err != nil {
		fmt.Printf("BigDataQuery error:%e\n", err)
	} else {
		fmt.Printf("Data count：%d row\n", len(data))
	}
	fmt.Printf("Takes :%dms\n", (time.Now().UnixNano()-time1)/1e6)
}

func ConcurrentQuery() {
	var time1 = time.Now().UnixNano()
	//查询一个月
	var beginTime int64 = int64(1262275200000 * time.Millisecond)
	var endTime int64 = int64(1264953600000 * time.Millisecond)
	var interval = OneMinute
	var wait = new(sync.WaitGroup)
	var threads = 100
	wait.Add(threads)
	for i := 0; i < threads; i++ {
		go func() {
			var _, err = QueryLines("BTCUSDT", interval, beginTime, endTime)
			if err != nil {
				fmt.Printf("ConcurrentQuery error:%e\n", err)

			} else {
				//fmt.Printf("查询数据数量：%d条\n", len(data))
			}
			wait.Done()
		}()
	}
	wait.Wait()
	fmt.Printf("Takes :%dms\n", (time.Now().UnixNano()-time1)/1e6)
}

func BigDataWrite() {
	var time1 = time.Now().UnixNano()
	var interval = OneMinute
	//插入10年分钟线数据
	var day = 3650
	//一天有多少分钟
	var daySize = 1140
	//80640
	//数据起始时间，北京时间2010 1月1日 0点
	var startTime int64 = int64(1262275200000 * time.Millisecond)
	var count int64 = 0
	var len = day * daySize
	var list = make([]Kline, len)
	for i := 0; i < len; i++ {
		list[i] = Kline{
			Flag:      1,
			Timestamp: startTime,
			DealCount: count,
			Open:      rand.Float64(),
			Close:     rand.Float64(),
			High:      rand.Float64(),
			Low:       rand.Float64(),
			Vol:       rand.Float64(),
			QuoteVol:  rand.Float64(),
		}
		startTime = interval.GetIntervalOffsetTime(startTime, 1)
		count++
	}
	var n, err = WriteKLines("BTCUSDT", interval, list...)
	if err != nil {
		fmt.Printf("Executed：%d row,error:%e\n", n, err)
		fmt.Printf("ConcurrentQuery error:%e\n", err)
	}
	var temp, _ = FileMapping["BTCUSDT-1m"].File.Stat()
	fmt.Printf("Executed：%d row,Takes: %dms\n", day*daySize, (time.Now().UnixNano()-time1)/1e6)
	fmt.Printf("Write:%dkb\n", day*daySize*KlineSize/1024)
	fmt.Printf("File Size:%dkb\n", temp.Size()/1024)
}
