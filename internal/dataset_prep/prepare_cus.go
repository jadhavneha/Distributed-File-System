package main

import (
	"encoding/csv"
	"fmt"
	"os"
)

const (
	InputCSV             = "customers-100000.csv" // Rename to match your source
	OutputCSV            = "customers.csv"
	MultiplicationFactor = 6 // Duplicate the data 100x
)

func main() {
	// 1. Open Input
	file, err := os.Open(InputCSV)
	if err != nil {
		fmt.Printf("Error opening input CSV: %v\n", err)
		return
	}
	defer file.Close()

	// 2. Read All Rows into Memory (Assuming source file is small < 10MB)
	reader := csv.NewReader(file)
	allRows, err := reader.ReadAll()
	if err != nil {
		fmt.Printf("Error reading CSV: %v\n", err)
		return
	}

	if len(allRows) == 0 {
		fmt.Println("Input CSV is empty.")
		return
	}

	header := allRows[0]
	dataRows := allRows[1:]

	// 3. Create Output
	outFile, err := os.Create(OutputCSV)
	if err != nil {
		fmt.Printf("Error creating output CSV: %v\n", err)
		return
	}
	defer outFile.Close()

	writer := csv.NewWriter(outFile)
	defer writer.Flush()

	fmt.Printf("Exploding %s by %dx...\n", InputCSV, MultiplicationFactor)

	// Write Header
	writer.Write(header)

	// Write Data Rows repeatedly
	for i := 0; i < MultiplicationFactor; i++ {
		prefix := fmt.Sprintf("%d-", i)

		for _, row := range dataRows {
			// Copy row to avoid modifying the original in memory
			newRow := make([]string, len(row))
			copy(newRow, row)

			// Prepend prefix to the first column (ID) to make it unique
			if len(newRow) > 0 {
				newRow[0] = prefix + newRow[0]
			}

			writer.Write(newRow)
		}
	}

	// Get file size stats
	stat, _ := outFile.Stat()
	fmt.Printf("Done! Created %s with size ~%.2f MB\n", OutputCSV, float64(stat.Size())/(1024*1024))
}
