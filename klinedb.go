package klinedb

import (
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"
)

const WorkDir = "data/klinedb/"

var FileMapping = make(map[string]*KlineFileInfo)
var Lock = new(sync.RWMutex)

//KlineSize Kline struct size - Timestamp = 8*8 byte
const KlineSize = 8 * 8

type Kline struct {
	Flag      int64
	Timestamp int64
	DealCount int64
	Open      float64
	Close     float64
	High      float64
	Low       float64
	Vol       float64
	QuoteVol  float64
}

func (t *Kline) IsEmpty() bool {
	return t.Flag&1 == 0
}

func EmptyKline() Kline {
	return Kline{Flag: 0}
}

type KlineFileInfo struct {
	EndIndex   int64
	WriteIndex int64
	Path       string
	File       *os.File
	StartTime  int64
	Interval   Interval
	EndTime    int64
	Lock       *sync.RWMutex
}

func (t *KlineFileInfo) Close() error {
	return t.File.Close()
}

func InitMapping() error {
	Lock.Lock()
	defer Lock.Unlock()
	var fileMapping = make(map[string]*KlineFileInfo)
	var err = os.MkdirAll(WorkDir, 0666)
	if err != nil {
		return err
	}
	var fileInfoList []os.FileInfo
	fileInfoList, err = ioutil.ReadDir(WorkDir)
	if err != nil {
		return err
	}
	for _, info := range fileInfoList {
		if info.IsDir() {
			continue
		}
		var name = info.Name()
		temp := strings.Split(name, "@")
		if len(temp) != 2 {
			continue
		}
		if info.Size()%KlineSize != 0 {
			return fmt.Errorf("file size error,size:%d", info.Size())
		}
		var startTime, _ = strconv.ParseInt(temp[1], 16, 64)
		var path = WorkDir + info.Name()
		var file, _ = os.OpenFile(path, os.O_RDWR, 0666)
		var interval = Intervals[strings.Split(temp[0], "-")[1]]
		fileMapping[temp[0]] = &KlineFileInfo{
			File:       file,
			Path:       path,
			EndIndex:   info.Size(),
			WriteIndex: info.Size(),
			StartTime:  startTime,
			Lock:       new(sync.RWMutex),
			EndTime:    interval.GetIntervalOffsetTime(startTime, (info.Size()/KlineSize)-1),
			Interval:   Intervals[strings.Split(temp[0], "-")[1]],
		}
	}
	FileMapping = fileMapping
	return nil
}

func getKlineFile(symbol string, interval Interval) (*KlineFileInfo, bool) {
	Lock.RLock()
	defer Lock.RUnlock()
	var id = fmt.Sprintf("%s-%s", symbol, interval.Interval)
	var val, ok = FileMapping[id]
	return val, ok
}

func createOrGetKlineFile(symbol string, interval Interval, firstKline Kline) (*KlineFileInfo, error) {
	Lock.RLock()
	var id = fmt.Sprintf("%s-%s", symbol, interval.Interval)
	var val, ok = FileMapping[id]
	Lock.RUnlock()
	var err error
	if !ok {
		Lock.Lock()
		defer Lock.Unlock()
		//multithreaded writing double check
		val, ok = FileMapping[id]
		if ok {
			return val, nil
		}

		var fileName = fmt.Sprintf("%s@%x", id, firstKline.Timestamp)
		var path = WorkDir + fileName
		var file *os.File
		file, err = os.OpenFile(path, os.O_RDWR|os.O_TRUNC|os.O_CREATE, 0666)
		if err != nil {
			return val, err
		}
		val = &KlineFileInfo{
			File:       file,
			Path:       path,
			EndIndex:   0,
			WriteIndex: 0,
			StartTime:  firstKline.Timestamp,
			Lock:       new(sync.RWMutex),
			EndTime:    firstKline.Timestamp,
			Interval:   interval,
		}
		FileMapping[id] = val
		return val, nil
	}
	return val, nil
}

//WriteKLines batch write
func WriteKLines(symbol string, interval Interval, klines ...Kline) (int64, error) {
	fileInfo, err := createOrGetKlineFile(symbol, interval, klines[0])
	if err != nil {
		return 0, err
	}
	fileInfo.Lock.Lock()
	defer fileInfo.Lock.Unlock()
	var writeCounter int64 = 0
	var bufSize int64 = KlineSize * 4096
	var byteBuffer = make([]byte, bufSize)
	var bufWriteIndex int64 = 0
	var klineLen = len(klines)

	for i, kline := range klines {
		if !interval.CheckTimestamp(kline.Timestamp) {
			return writeCounter, fmt.Errorf("timestamp does not match interval,time:%d,interval:%s", kline.Timestamp, interval.Interval)
		}
		if fileInfo.StartTime > kline.Timestamp {
			return -1, fmt.Errorf("timestamp less than start time,timestamp:%d,startTime:%d", kline.Timestamp, fileInfo.StartTime)
		}
		var currOffset = calcOffset(fileInfo, kline.Timestamp, interval)
		//if write offset!=current offset，re-seek
		if fileInfo.WriteIndex != currOffset {
			if bufWriteIndex != 0 {
				_, err = fileInfo.File.Write(byteBuffer[0:bufWriteIndex])
				if err != nil {
					return writeCounter, err
				}
				bufWriteIndex = 0
			}
			_, err = fileInfo.File.Seek(currOffset, io.SeekStart)
			if err != nil {
				return writeCounter, err
			}
			fileInfo.WriteIndex = currOffset
		}

		Kline2Bytes(kline, byteBuffer, bufWriteIndex)
		bufWriteIndex += KlineSize
		fileInfo.WriteIndex += KlineSize
		if fileInfo.WriteIndex > fileInfo.EndIndex {
			fileInfo.EndIndex = fileInfo.WriteIndex
		}
		if fileInfo.EndTime < kline.Timestamp {
			fileInfo.EndTime = kline.Timestamp
		}
		if bufWriteIndex >= bufSize || (i == klineLen-1 && bufWriteIndex > 0) {
			_, err = fileInfo.File.Write(byteBuffer[:bufWriteIndex])
			if err != nil {
				return writeCounter, err
			}
			bufWriteIndex = 0
		}
		writeCounter++
	}
	return writeCounter, nil
}

//QueryLines Time range query
func QueryLines(symbol string, interval Interval, begin int64, end int64) ([]Kline, error) {
	if begin > end {
		return nil, fmt.Errorf("begin time greater than end time,begin:%d,end:%d", begin, end)
	}
	fileInfo, ok := getKlineFile(symbol, interval)
	if !ok {
		return nil, fmt.Errorf("data does not exist:%s-%s", symbol, interval.Interval)
	}
	fileInfo.Lock.RLock()
	defer fileInfo.Lock.RUnlock()
	//Align query time with interval
	//flooring
	begin = interval.Floor(begin)
	//ceiling
	end = interval.Ceil(end)

	var offset, err = calcQueryOffset(fileInfo, begin, interval)
	if err != nil {
		return nil, err
	}
	var preNum = interval.CalcRangeInDataCount(begin, end)

	var readSize = preNum * KlineSize
	//If query time > end time { query time = end time}
	if readSize+offset > fileInfo.EndIndex {
		readSize = fileInfo.EndIndex - offset
	}
	var bytes = make([]byte, readSize)
	var n int
	n, err = fileInfo.File.ReadAt(bytes, offset)
	var preKlineCount = n / KlineSize
	var list = make([]Kline, 0, preKlineCount)
	if err != nil {
		return nil, err
	}
	var byteOffset int64 = 0
	for i := 0; i < preKlineCount; i++ {
		var item, err = Bytes2Kline(bytes, byteOffset)
		if err != nil {
			return nil, err
		}
		if item.IsEmpty() {
			byteOffset += KlineSize
			continue
		}
		//Calculate the timestamp of the data by the offset
		item.Timestamp = interval.GetIntervalOffsetTime(fileInfo.StartTime, (offset+byteOffset)/KlineSize)
		list = append(list, *item)
		byteOffset += KlineSize
	}
	return list, nil
}

//calcQueryOffset Calculate the offset of the data by the timestamp and check query time
func calcQueryOffset(fileInfo *KlineFileInfo, time int64, interval Interval) (int64, error) {
	if fileInfo.StartTime > time {
		return -1, fmt.Errorf("query time less than start time,queryTime:%d,startTime:%d", time, fileInfo.StartTime)
	}
	if fileInfo.EndTime < time {
		return -1, fmt.Errorf("query time greater than start time,queryTime:%d,startTime:%d", time, fileInfo.StartTime)
	}
	if !interval.CheckTimestamp(fileInfo.StartTime) {
		return -1, fmt.Errorf("file start time does not match interval,time:%d,interval:%s", time, interval.Interval)

	}
	if !interval.CheckTimestamp(time) {
		return -1, fmt.Errorf("query time does not match interval,time:%d,interval:%s", time, interval.Interval)
	}
	return calcOffset(fileInfo, time, interval), nil
}

//calcOffset Calculate the offset of the data by the timestamp
func calcOffset(fileInfo *KlineFileInfo, time int64, interval Interval) int64 {
	return interval.CalcIndex(time-fileInfo.StartTime) * KlineSize
}

//Bytes2Kline decode data
func Bytes2Kline(bytes []byte, offset int64) (*Kline, error) {
	if len(bytes)%KlineSize != 0 {
		return nil, fmt.Errorf("bytes2Kline error,bytes size%%%d!=0", KlineSize)
	}
	var kline = new(Kline)
	kline.Flag = int64(binary.BigEndian.Uint64(bytes[offset : offset+8]))
	kline.DealCount = int64(binary.BigEndian.Uint64(bytes[offset+8 : offset+16]))
	kline.Open = math.Float64frombits(binary.BigEndian.Uint64(bytes[offset+16 : offset+24]))
	kline.Close = math.Float64frombits(binary.BigEndian.Uint64(bytes[offset+24 : offset+32]))
	kline.High = math.Float64frombits(binary.BigEndian.Uint64(bytes[offset+32 : offset+40]))
	kline.Low = math.Float64frombits(binary.BigEndian.Uint64(bytes[offset+40 : offset+48]))
	kline.Vol = math.Float64frombits(binary.BigEndian.Uint64(bytes[offset+48 : offset+56]))
	kline.QuoteVol = math.Float64frombits(binary.BigEndian.Uint64(bytes[offset+56 : offset+64]))
	return kline, nil
}

//Kline2Bytes encode data
func Kline2Bytes(kline Kline, bytes []byte, offset int64) {
	var buf = make([]byte, 8)
	//use offset calc timestamp
	//binary.BigEndian.PutUint64(buf,uint64(klinedb.Timestamp))
	//buffer.Write(buf)
	binary.BigEndian.PutUint64(buf, uint64(kline.Flag))
	copy(bytes[offset:offset+8], buf)
	binary.BigEndian.PutUint64(buf, uint64(kline.DealCount))
	copy(bytes[offset+8:offset+16], buf)
	binary.BigEndian.PutUint64(buf, math.Float64bits(kline.Open))
	copy(bytes[offset+16:offset+24], buf)
	binary.BigEndian.PutUint64(buf, math.Float64bits(kline.Close))
	copy(bytes[offset+24:offset+32], buf)
	binary.BigEndian.PutUint64(buf, math.Float64bits(kline.High))
	copy(bytes[offset+32:offset+40], buf)
	binary.BigEndian.PutUint64(buf, math.Float64bits(kline.Low))
	copy(bytes[offset+40:offset+48], buf)
	binary.BigEndian.PutUint64(buf, math.Float64bits(kline.Vol))
	copy(bytes[offset+48:offset+56], buf)
	binary.BigEndian.PutUint64(buf, math.Float64bits(kline.QuoteVol))
	copy(bytes[offset+56:offset+64], buf)
}
