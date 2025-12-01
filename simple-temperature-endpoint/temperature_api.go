package temperature_api

import (
	"errors"
	"sort"
	"sync"
	"time"
)

const MaxDataPoints = 10000

type TemperatureApi struct {
	Data  map[string]DeviceData
	mutex sync.RWMutex
}

type DeviceData struct {
	DeviceType string      `json:"device_type"`
	DataPoints []DataPoint `json:"data_points"`
}

type ResponseBody struct {
	DeviceData
	DeviceID string `json:"device_id"`
}

type DataPoint struct {
	Timestamp   time.Time `json:"timestamp"`
	Temperature float64   `json:"temperature"`
}

func (server *TemperatureApi) Init() {
	server.Data = make(map[string]DeviceData)
}

func (server *TemperatureApi) PostTemperature(sensorID, deviceType string, sampleTime time.Time, temperature float64) error {

	// validate input data
	if sensorID == "" {
		return errors.New("device ID cannot be empty")
	}
	if deviceType == "" {
		return errors.New("device type cannot be empty")
	}

	// if this was a for-realz API, it would be called concurrently from
	// the HTTP handlers, so we need to lock the data structure
	server.mutex.Lock()
	defer server.mutex.Unlock()

	deviceData := server.Data[sensorID] // Go returns zero value if key not found

	// set device type (in case this is the first time we've seen this sensor ID)
	// this could be problematic if we had an unreliable sensor that was sending
	// different device types for the same sensor ID though...
	deviceData.DeviceType = deviceType
	deviceData.DataPoints = append(deviceData.DataPoints, DataPoint{
		Timestamp:   sampleTime,
		Temperature: temperature,
	})

	// sort data by timestamp
	// note that this might not be the most efficient way to keep this data sorted,
	// but with this approach we sort once on insert, vs. sorting on each GET
	sort.Slice(deviceData.DataPoints, func(i, j int) bool {
		return deviceData.DataPoints[i].Timestamp.Before(deviceData.DataPoints[j].Timestamp)
	})

	server.Data[sensorID] = deviceData

	return nil
}

func (server *TemperatureApi) GetTemperature(deviceID string, startTime, endTime time.Time) (ResponseBody, error) {

	// validate input data
	if deviceID == "" {
		return ResponseBody{}, errors.New("device ID cannot be empty")
	}

	if _, ok := server.Data[deviceID]; !ok {
		// no data for this sensor
		return ResponseBody{}, errors.New("no data for this device ID")
	}

	if endTime.Before(startTime) {
		return ResponseBody{}, errors.New("end time cannot be before start time")
	}

	// Read-lock the data structure for concurrent safety
	server.mutex.RLock()
	defer server.mutex.RUnlock()

	// retrieve and slice data
	data := server.Data[deviceID]

	// return empty data if the device exists but there are no
	// temperature data points for it
	// (this shouldn't be possible, but just in case)
	if len(data.DataPoints) == 0 {
		return ResponseBody{
			DeviceID: deviceID,
			DeviceData: DeviceData{
				DeviceType: data.DeviceType,
				DataPoints: []DataPoint{},
			},
		}, nil
	}

	// Before doing a binary search, do a check on the data to see
	// if the request is outside of the available data
	left := data.DataPoints[0].Timestamp                       // first timestamp available
	right := data.DataPoints[len(data.DataPoints)-1].Timestamp // final timestamp available

	// Case 1: requested range is full outside of the available data
	//         this is defined as the start time coming AFTER the most-srecent data point
	//         or the end time coming BEFORE the earliest data point
	if startTime.After(right) || endTime.Before(left) {
		return ResponseBody{}, nil
	}

	// use Go's build-in binary search to find the smallest
	// and largest indicies for the requested time range
	// Go will return Len(data) if we search for a time that is beyond
	// the timestamps available and will return 0 if we search for a
	// lower time stamp. This works for us as we want to return
	// data within the requested time range.
	startIndex := sort.Search(len(data.DataPoints), func(i int) bool {
		return !data.DataPoints[i].Timestamp.Before(startTime)
	})

	endIndex := sort.Search(len(data.DataPoints), func(i int) bool {
		return data.DataPoints[i].Timestamp.After(endTime)
	})

	// return the slice of data points within the requested time range
	return ResponseBody{
		DeviceID: deviceID,
		DeviceData: DeviceData{
			DeviceType: data.DeviceType,
			DataPoints: data.DataPoints[startIndex:endIndex],
		},
	}, nil
}
