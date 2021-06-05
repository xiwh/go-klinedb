# Go-KlineDB
**Super fast k-line time series database**

It is a very mini k-line time series database, providing simple but very efficient querying and writing and storage functions
## Test result
- SSD local test result:
```
Batch write 10-year minute line data:
Executed：4161000 row,Takes: 733ms
Write:260062kb
File Size:260062kb
Single-thread query of 5-year minute-line data:
Data count：2629441 row
Takes :262ms
100 concurrent queries for 1 month's minute-line data:
Takes :140ms
```
- Centos 2CoreCPU 8GRAM HDD result:
```
Batch write 10-year minute line data:
Executed：4161000 row,Takes: 966ms
Write:260062kb
File Size:260062kb
Single-thread query of 5-year minute-line data:
Data count：2629441 row
Takes :242ms
100 concurrent queries for 1 month's minute-line data:
Takes :240ms
```
## Usage

```go
package main

import (
	"fmt"
	"klinedb"
	"math/rand"
)

func Init()  {
	klinedb.InitMapping()
}

func Insert() {
	var item1 = klinedb.Kline{
		Flag:      1,
		Timestamp: klinedb.OneMinute.GetIntervalOffsetTime(1262275200000, 100),
		DealCount: 1335345436,
		Open:      rand.Float64(),
		Close:     rand.Float64(),
		High:      rand.Float64(),
		Low:       rand.Float64(),
		Vol:       rand.Float64(),
		QuoteVol:  rand.Float64(),
	}
	var item2 = klinedb.Kline{
		Flag:      1,
		Timestamp: klinedb.OneMinute.GetIntervalOffsetTime(1262275200000, 105),
		DealCount: 55555555,
		Open:      rand.Float64(),
		Close:     rand.Float64(),
		High:      rand.Float64(),
		Low:       rand.Float64(),
		Vol:       rand.Float64(),
		QuoteVol:  rand.Float64(),
	}
	var count, err = klinedb.WriteKLines("BTCUSDT", klinedb.OneMinute, item1, item2)
	if err != nil {
		fmt.Printf("insert:%d", count)
	} else {
		fmt.Printf("insert err:%e", err)
	}
}

func Query() {
	var list , err = klinedb.QueryLines("BTCUSDT", klinedb.OneMinute, klinedb.OneMinute.GetIntervalOffsetTime(1262275200000, 100), klinedb.OneMinute.GetIntervalOffsetTime(1262275200000, 105))
	if err != nil {
		fmt.Printf("query:%d", len(list))
	} else {
		fmt.Printf("query err:%e", err)
	}
}
```