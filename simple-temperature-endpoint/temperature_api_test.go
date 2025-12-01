package temperature_api

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetTemperature(t *testing.T) {
	// Arrange
	target := TemperatureApi{}
	target.Init()
	deviceID := "device1"
	deviceType := "temperature_sensor"
	startTime := time.Now()
	sampleIntervalSeconds := 5

	// Create sample data
	expectedSampleCount := 1000
	base_temperature_celsius := 20.0
	sampleData := make([]DataPoint, expectedSampleCount)

	// insert the data points in reverse order to validate sorting
	for i := expectedSampleCount - 1; i >= 0; i-- {
		sampleData[i] = DataPoint{
			Timestamp:   startTime.Add(time.Second * time.Duration(i*sampleIntervalSeconds)),
			Temperature: math.Sin(float64(i)*.01) + base_temperature_celsius,
		}
	}

	// Act
	// Post all data points
	for i := 0; i < expectedSampleCount; i++ {
		target.PostTemperature(deviceID, deviceType, sampleData[i].Timestamp, sampleData[i].Temperature)
	}

	// capture the time of the final sample
	endTime := sampleData[expectedSampleCount-1].Timestamp
	actual, err := target.GetTemperature(deviceID, startTime, endTime)

	// Assert
	assert.NoError(t, err)
	assert.Len(t, actual.DataPoints, expectedSampleCount)
	assert.Equal(t, startTime, actual.DataPoints[0].Timestamp)
	assert.Equal(t, endTime, actual.DataPoints[expectedSampleCount-1].Timestamp)
	assert.Equal(t, sampleData, actual.DataPoints)
}

// Test input validation
func TestPostTemperature_InputValidation(t *testing.T) {
	// Arrange
	target := TemperatureApi{}
	target.Init()
	sampleTime := time.Now()
	temperature := 25.0

	// Act & Assert
	err := target.PostTemperature("", "temperature_sensor", sampleTime, temperature)
	assert.NotNil(t, err)
	assert.Equal(t, "device ID cannot be empty", err.Error())

	err = target.PostTemperature("device1", "", sampleTime, temperature)
	assert.NotNil(t, err)
	assert.Equal(t, "device type cannot be empty", err.Error())

	err = target.PostTemperature("device1", "temperature_sensor", sampleTime, temperature)
	assert.Nil(t, err)
}

func TestGetTemperature_InputValidation(t *testing.T) {
	// Arrange
	target := TemperatureApi{}
	target.Init()
	mockDeviceID := "device1"
	startTime := time.Now()
	endTime := startTime.Add(time.Minute)

	// Act - no data for device
	actual, err := target.GetTemperature(mockDeviceID, startTime, endTime)

	// Assert
	assert.Error(t, err)
	assert.Empty(t, actual)

	// Act & Assert
	err = target.PostTemperature(mockDeviceID, "temperature_sensor", startTime, 25.0)
	require.NoError(t, err)

	// request an invalid duration (end before start)
	_, err = target.GetTemperature(mockDeviceID, endTime, startTime)
	assert.NotNil(t, err)
	assert.Equal(t, "end time cannot be before start time", err.Error())
}

func TestGetTemperature_Ranges(t *testing.T) {
	// Arrange
	target := TemperatureApi{}
	target.Init()
	deviceID := "device1"
	deviceType := "temperature_sensor"
	startTime := time.Now()
	sampleIntervalSeconds := 10

	// Create sample data
	totalSampleCount := 1000
	base_temperature_celsius := 20.0

	sampleData := make([]DataPoint, totalSampleCount)

	// create some mock data
	for i := 0; i < totalSampleCount; i++ {
		sampleData[i] = DataPoint{
			Timestamp:   startTime.Add(time.Second * time.Duration(i*sampleIntervalSeconds)),
			Temperature: math.Sin(float64(i)*.01) + base_temperature_celsius,
		}

		target.PostTemperature(deviceID, deviceType, sampleData[i].Timestamp, sampleData[i].Temperature)
	}

	endTime := sampleData[totalSampleCount-1].Timestamp

	// Act
	// Request data for an actual device but for a time range that doesn't exist (left)
	actual, err := target.GetTemperature(deviceID, endTime.Add(time.Hour), endTime.Add(2*time.Hour))

	// Assert
	assert.NoError(t, err)
	assert.Empty(t, actual)

	// Act - Request data for an actual device but for a time range that comes completely BEFORE
	//        the available data
	actual, err = target.GetTemperature(deviceID, startTime.Add(-2*time.Hour), startTime.Add(-time.Hour))

	// Assert
	assert.NoError(t, err)
	assert.Empty(t, actual)

	// Act - Request a single-point of data (first sample)
	actual, err = target.GetTemperature(deviceID, startTime, startTime)

	// Assert
	assert.NoError(t, err)
	assert.Len(t, actual.DataPoints, 1)
	assert.Equal(t, startTime, actual.DataPoints[0].Timestamp)

	// Act - Request a single-point of data (last sample)
	actual, err = target.GetTemperature(deviceID, endTime, endTime)

	// Assert
	assert.NoError(t, err)
	assert.Len(t, actual.DataPoints, 1)
	assert.Equal(t, endTime, actual.DataPoints[0].Timestamp)

	// Act - Request a time range for which we have data
	actual, err = target.GetTemperature(deviceID, startTime, endTime)

	// Assert
	assert.NoError(t, err)
	assert.Len(t, actual.DataPoints, totalSampleCount)
	assert.Equal(t, sampleData, actual.DataPoints)

	// Act - request a superset of the available data
	actual, err = target.GetTemperature(deviceID, startTime.Add(-time.Hour), endTime.Add(time.Hour))

	// Assert
	assert.NoError(t, err)
	assert.Len(t, actual.DataPoints, totalSampleCount)
	assert.Equal(t, sampleData, actual.DataPoints)

	// Act - request a time range that starts before the available data, but ends in range
	//       of the available data
	sampleCount := 10
	actual, err = target.GetTemperature(deviceID, startTime.Add(-time.Hour), startTime.Add(time.Duration(sampleCount*sampleIntervalSeconds)*time.Second))

	// Assert
	assert.NoError(t, err)
	assert.Len(t, actual.DataPoints, sampleCount+1)
	assert.Equal(t, sampleData[0:sampleCount+1], actual.DataPoints)

	// Act - request a time range that starts in range of the available data, but ends after
	//       the available data
	actual, err = target.GetTemperature(deviceID, endTime.Add(-time.Duration(sampleCount*sampleIntervalSeconds)*time.Second), endTime.Add(time.Hour))

	// Assert
	assert.NoError(t, err)
	assert.Len(t, actual.DataPoints, sampleCount+1)
	assert.Equal(t, sampleData[totalSampleCount-sampleCount-1:totalSampleCount], actual.DataPoints)

	// Act - boundary conditions - request the final data point + 1 hour
	actual, err = target.GetTemperature(deviceID, sampleData[len(sampleData)-1].Timestamp, endTime.Add(time.Hour))

	// Assert
	assert.NoError(t, err)
	assert.Len(t, actual.DataPoints, 1)
	assert.Equal(t, sampleData[len(sampleData)-1], actual.DataPoints[0])
}
