package klinedb

import "time"

var OneMS = NewInterval("1ms", int64(time.Millisecond))
var OneSecond = NewInterval("1s", int64(time.Second))
var OneMinute = NewInterval("1m", int64(time.Minute))
var ThreeMinute = NewInterval("3m", int64(time.Minute*3))
var FiveMinute = NewInterval("5m", int64(time.Minute*5))
var FifteenMinute = NewInterval("15m", int64(time.Minute*15))
var ThirtyMinute = NewInterval("30m", int64(time.Minute*30))
var OneHour = NewInterval("1h", int64(time.Hour))
var TwoHour = NewInterval("2h", int64(time.Hour*2))
var FourHour = NewInterval("4h", int64(time.Hour*4))
var SixHour = NewInterval("6h", int64(time.Hour*6))
var EightHour = NewInterval("8h", int64(time.Hour*8))
var TwelveHour = NewInterval("12h", int64(time.Hour*12))
var OneDay = NewInterval("1d", int64(time.Hour*24))
var ThreeDay = NewInterval("3d", int64(time.Hour*24*3))
var OneWeek = NewInterval("1w", int64(time.Hour*24*7))
var OneMonth = Interval{Interval: "1M", Method: MonthTimelineIndexingMethod{}}
var Intervals = map[string]Interval{
	OneMS.Interval:         OneMS,
	OneSecond.Interval:     OneSecond,
	OneMinute.Interval:     OneMinute,
	ThreeMinute.Interval:   ThreeMinute,
	FiveMinute.Interval:    FiveMinute,
	FifteenMinute.Interval: FifteenMinute,
	ThirtyMinute.Interval:  ThirtyMinute,
	OneHour.Interval:       OneHour,
	TwoHour.Interval:       TwoHour,
	FourHour.Interval:      FourHour,
	SixHour.Interval:       SixHour,
	EightHour.Interval:     EightHour,
	TwelveHour.Interval:    TwelveHour,
	OneDay.Interval:        OneDay,
	ThreeDay.Interval:      ThreeDay,
	OneWeek.Interval:       OneWeek,
	OneMonth.Interval:      OneMonth,
}

type TimelineIndexingMethod interface {
	CalcIndex(timestamp int64) int64
	CalcTimestamp(index int64) int64
	CalcRangeInDataCount(begin int64, end int64) int64
	CheckTimestamp(timestamp int64) bool
	GetIntervalOffsetTime(timestamp int64, intervals int64) int64
	Floor(timestamp int64) int64
	Ceil(timestamp int64) int64
}

type DefaultTimelineIndexingMethod struct {
	IntervalTime int64
}

func (t DefaultTimelineIndexingMethod) CalcIndex(timestamp int64) int64 {
	return timestamp / t.IntervalTime
}
func (t DefaultTimelineIndexingMethod) CalcTimestamp(index int64) int64 {
	return index * t.IntervalTime
}
func (t DefaultTimelineIndexingMethod) CalcRangeInDataCount(begin int64, end int64) int64 {
	return (end-begin)/t.IntervalTime + 1
}
func (t DefaultTimelineIndexingMethod) CheckTimestamp(timestamp int64) bool {
	return timestamp%t.IntervalTime == 0
}
func (t DefaultTimelineIndexingMethod) GetIntervalOffsetTime(timestamp int64, intervals int64) int64 {
	return timestamp + (intervals * t.IntervalTime)
}
func (t DefaultTimelineIndexingMethod) Floor(timestamp int64) int64 {
	return timestamp - (timestamp % t.IntervalTime)
}
func (t DefaultTimelineIndexingMethod) Ceil(timestamp int64) int64 {
	var temp = timestamp % t.IntervalTime
	if temp != 0 {
		return timestamp + t.IntervalTime - temp
	}
	return timestamp
}

type MonthTimelineIndexingMethod struct {
}

func (t MonthTimelineIndexingMethod) CalcIndex(timestamp int64) int64 {
	date := time.Unix(timestamp/1000, 0)
	return int64(date.Year()*12) + int64(date.Month())
}
func (t MonthTimelineIndexingMethod) CalcTimestamp(index int64) int64 {
	year := index / 12
	month := index % 12
	date := time.Date(int(year), time.Month(month), 1, 0, 0, 0, 0, time.Local)
	return date.UnixNano()
}
func (t MonthTimelineIndexingMethod) CalcRangeInDataCount(begin int64, end int64) int64 {
	return t.CalcIndex(end) - t.CalcIndex(begin) + 1
}
func (t MonthTimelineIndexingMethod) CheckTimestamp(timestamp int64) bool {
	return true
}
func (t MonthTimelineIndexingMethod) GetIntervalOffsetTime(timestamp int64, intervals int64) int64 {
	date := time.Unix(timestamp/1000, 0)
	if date.Day() > 1 {
		date = date.AddDate(0, int(intervals), -(date.Day() - 1))
	} else {
		date = date.AddDate(0, int(intervals), 0)
	}
	return date.UnixNano()
}
func (t MonthTimelineIndexingMethod) Floor(timestamp int64) int64 {
	return timestamp
}
func (t MonthTimelineIndexingMethod) Ceil(timestamp int64) int64 {
	return timestamp
}

type Interval struct {
	Interval string
	Method   TimelineIndexingMethod
}

func (t Interval) CalcIndex(timestamp int64) int64 {
	return t.Method.CalcIndex(timestamp)
}
func (t Interval) CalcTimestamp(index int64) int64 {
	return t.Method.CalcTimestamp(index)
}
func (t Interval) CalcRangeInDataCount(begin int64, end int64) int64 {
	return t.Method.CalcRangeInDataCount(begin, end)
}
func (t Interval) CheckTimestamp(timestamp int64) bool {
	return t.Method.CheckTimestamp(timestamp)
}
func (t Interval) GetIntervalOffsetTime(timestamp int64, intervals int64) int64 {
	return t.Method.GetIntervalOffsetTime(timestamp, intervals)
}
func (t Interval) Floor(timestamp int64) int64 {
	return t.Method.Floor(timestamp)
}
func (t Interval) Ceil(timestamp int64) int64 {
	return t.Method.Ceil(timestamp)
}
func NewInterval(name string, intervalTime int64) Interval {
	return Interval{Interval: name, Method: DefaultTimelineIndexingMethod{IntervalTime: intervalTime}}
}
