package formatter

import (
	"encoding/csv"
	"io"
	"log"
	"os"
)

type AbstractCsvFormatter struct {
	FileName string
	Logger   *log.Logger
	Headers  []string
	Rows     [][]string
}

func NewAbstractCsvFormatter(logger *log.Logger) *AbstractCsvFormatter {
	return &AbstractCsvFormatter{
		Logger: logger,
	}
}

func (acf *AbstractCsvFormatter) Supports(fileName string) bool {
	return acf.FileName == fileName
}

func (acf *AbstractCsvFormatter) ReadFile(path string) ([]map[string]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.Comma = ';'
	reader.FieldsPerRecord = -1

	var rows []map[string]string
	var rowNumber int

	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		if rowNumber == 0 {
			acf.Headers = row
		} else {
			if len(acf.Headers) != len(row) {
				acf.Logger.Printf("Found different lengths in header (%d) and row (%d). HEADER: %v. ROW: %v\n", len(acf.Headers), len(row), acf.Headers, row)
				for len(row) < len(acf.Headers) {
					row = append(row, "")
				}
			}
			rowMap := acf.GetRow(acf.zip(acf.Headers, row))
			rows = append(rows, rowMap)
		}
		rowNumber++
	}
	return rows, nil
}

func (acf *AbstractCsvFormatter) GetRow(data map[string]string) map[string]string {
	return data
}

func (acf *AbstractCsvFormatter) GetCsv(file io.Reader) ([]string, error) {
	reader := csv.NewReader(file)
	reader.Comma = ';'
	return reader.Read()
}

func (acf *AbstractCsvFormatter) zip(headers []string, row []string) map[string]string {
	data := make(map[string]string)
	for i, header := range headers {
		data[header] = row[i]
	}
	return data
}
