package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"sort"
	"time"
)

// ------------------------------------------------------- Universal Types

type Structure struct {
	Date string
	Lat  float64
	Lng  float64
	Hour int
}

type ComputedStructure struct {
	Date       string
	Lat        float64
	Lng        float64
	Hour       int
	SunsetHour int
}

// ------------------------------------------------------- Main Worker

func main() {
	log.SetFlags(0)
	if len(os.Args) < 2 {
		log.Fatalf("Usage: go run main.go <input file path>")
	}

	inputFilePath := os.Args[1]
	outputFilePath := "./IFF1-5_BradauskasA_L1_rez.txt"
	workerCount := 10

	// Start timer
	startTime := time.Now()

	initializePipeline(inputFilePath, outputFilePath, workerCount)

	// Calculate elapsed time
	elapsedTime := time.Since(startTime)
	fmt.Printf("Pipeline executed in: %s\n", elapsedTime)

	fmt.Println("Application has stopped running")
}

func initializePipeline(inputFilePath, outputFilePath string, workerCount int) {
	dataMonitorSendDataChannel := make(chan Structure)
	dataMonitorRequestDataChannel := make(chan bool, workerCount)
	dataMonitorResponseChannel := make(chan Structure)
	noMoreDataChannel := make(chan bool)

	resultMonitorSendDataChannel := make(chan ComputedStructure)
	resultMonitorSendFinalDataChannel := make(chan []ComputedStructure)
	workerStatusChannel := make(chan bool)

	data, err := ReadJSON(inputFilePath)
	if err != nil {
		log.Fatalf("Error: %v", err)
	}

	fmt.Println("Starting Data Monitor")
	go InitDataMonitor(dataMonitorSendDataChannel, dataMonitorRequestDataChannel, dataMonitorResponseChannel, noMoreDataChannel)

	fmt.Println("Starting Result Monitor")
	go InitResultMonitor(resultMonitorSendDataChannel, resultMonitorSendFinalDataChannel, workerCount, workerStatusChannel)

	fmt.Println("Starting Workers Count:", workerCount)
	for i := 1; i <= workerCount; i++ {
		fmt.Println("Creating Worker:", i)
		go InitWorker(dataMonitorRequestDataChannel, dataMonitorResponseChannel, resultMonitorSendDataChannel, workerStatusChannel)
	}

	fmt.Println("Inserting Data")
	for _, value := range data {
		dataMonitorSendDataChannel <- value
	}
	noMoreDataChannel <- true

	computedData := <-resultMonitorSendFinalDataChannel

	writeOutputToFile(outputFilePath, computedData)
	fmt.Println("Application has stopped running")
}

func writeOutputToFile(outputFilePath string, computedData []ComputedStructure) {
	outputFile, err := os.Create(outputFilePath)
	if err != nil {
		log.Fatalf("Error creating output file: %v", err)
	}
	defer outputFile.Close()

	for _, value := range computedData {
		fmt.Fprintf(outputFile, "Date: %s Lat: %.2f Lng: %.2f Guess Hour: %d Sunset Hour: %d\n", value.Date, value.Lat, value.Lng, value.Hour, value.SunsetHour)
	}
	fmt.Fprintf(outputFile, "Count: %d\n", len(computedData))
}

// ReadJSON reads data from a JSON file and returns it
func ReadJSON(filePath string) ([]Structure, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var inputData []Structure
	decoder := json.NewDecoder(file)
	err = decoder.Decode(&inputData)
	if err != nil {
		return nil, err
	}

	return inputData, nil
}

// InitDataMonitor handles the data insertion and retrieval for workers
func InitDataMonitor(sendDataChannel chan Structure, requestDataChannel chan bool, responseDataChannel chan Structure, noMoreDataChannel chan bool) {
	// Simplified the data array to use slice
	var data []Structure
	noMoreData := false

	for {
		select {
		case insertObject := <-sendDataChannel:
			data = append(data, insertObject)

		case <-requestDataChannel:
			if len(data) > 0 {
				// Retrieve the first element and update the slice
				responseDataChannel <- data[0]
				data = data[1:]
			}

		case <-noMoreDataChannel:
			noMoreData = true

		default:
			if noMoreData && len(data) == 0 {
				close(responseDataChannel)
				return
			}
		}
	}
}

// InitResultMonitor handles the accumulation and sorting of results
func InitResultMonitor(resultMonitorSendDataChannel chan ComputedStructure, resultMonitorSendFinalDataChannel chan []ComputedStructure, workerCount int, workerStatusChannel chan bool) {
	var data []ComputedStructure

	for workerCount > 0 {
		select {
		case resultData := <-resultMonitorSendDataChannel:
			data = append(data, resultData)
			sort.Slice(data, func(i, j int) bool {
				return data[i].Hour < data[j].Hour
			})

		case <-workerStatusChannel:
			workerCount--
		}
	}

	resultMonitorSendFinalDataChannel <- data
}

// InitWorker represents a single worker fetching and processing data
func InitWorker(requestDataChannel chan bool, responseDataChannel chan Structure, resultMonitorSendDataChannel chan ComputedStructure, workerStatusChannel chan bool) {
	for {
		requestDataChannel <- true
		data, more := <-responseDataChannel
		if !more {
			break
		}

		sunsetHour, err := FindSunsetHour(data)
		if err != nil {
			fmt.Printf("Error finding sunset hour: %v\n", err)
			continue
		}

		if sunsetHour != data.Hour {
			continue
		}

		resultMonitorSendDataChannel <- ComputedStructure{
			Date:       data.Date,
			Lat:        data.Lat,
			Lng:        data.Lng,
			Hour:       data.Hour,
			SunsetHour: sunsetHour,
		}
	}

	workerStatusChannel <- true
}

// FindSunsetHour makes an API call to get the sunset time for a given location and date
func FindSunsetHour(data Structure) (int, error) {
	client := &http.Client{Timeout: 10 * time.Second}
	apiURL := fmt.Sprintf("https://api.sunrise-sunset.org/json?lat=%f&lng=%f&date=%s", data.Lat, data.Lng, data.Date)

	resp, err := client.Get(apiURL)
	if err != nil {
		return -1, err
	}
	defer resp.Body.Close()

	var result apiResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return -1, err
	}

	if result.Status != "OK" {
		return -1, fmt.Errorf("api returned status: %s", result.Status)
	}

	sunsetTime, err := time.Parse("3:04:05 PM", result.Results.Sunset)
	if err != nil {
		return -1, err
	}

	return sunsetTime.Hour(), nil
}

type apiResponse struct {
	Results struct {
		Sunset string `json:"sunset"`
	} `json:"results"`
	Status string `json:"status"`
}
