package formatter

import (
	"strings"
	"time"
	"log"
)

// AbstractDataFormatter represents the abstract data formatter
type AbstractDataFormatter struct {
	PathsMapping        map[string]string
	TypesMapping        map[string]func(string) interface{}
	DatesMapping        map[string][3]string
	NullEmptyFieldMap   []string
	RemoveFieldMap      []string
	NotDefinedFieldMap  map[string]string
	RenameFieldMap      map[string]string
	CodeNotDefined      string
	LabelNotDefined     string
}

// NewAbstractDataFormatter creates a new instance of AbstractDataFormatter
func NewAbstractDataFormatter() *AbstractDataFormatter {
	return &AbstractDataFormatter{
		PathsMapping:       make(map[string]string),
		TypesMapping:       make(map[string]func(string) interface{}),
		DatesMapping:       make(map[string][3]string),
		NullEmptyFieldMap:  []string{},
		RemoveFieldMap:     []string{},
		NotDefinedFieldMap: make(map[string]string),
		RenameFieldMap:     make(map[string]string),
		CodeNotDefined:     "NonDefini",
		LabelNotDefined:    "Non défini",
	}
}

// FormatPath formats the paths based on PathsMapping
func (adf *AbstractDataFormatter) FormatPath(payload map[string]string, formattedPayload map[string]interface{}) map[string]interface{} {
	for source, destination := range adf.PathsMapping {
		value := payload[source]
		formattedPayload[destination] = value
	}
	return formattedPayload
}

// FormatType formats the types based on TypesMapping
func (adf *AbstractDataFormatter) FormatType(formattedPayload map[string]interface{}) map[string]interface{} {
	for source, transform := range adf.TypesMapping {
		value := formattedPayload[source].(string)
		formattedPayload[source] = transform(value)
	}
	return formattedPayload
}

// FormatDate formats the dates based on DatesMapping
func (adf *AbstractDataFormatter) FormatDate(formattedPayload map[string]interface{}) map[string]interface{} {
	for source, dateConf := range adf.DatesMapping {
		value := formattedPayload[source].(string)
		if value == "" {
			continue
		}
		if strings.Contains(value, ".") && len(strings.Split(value, ".")[1]) == 7 {
			value = value[:len(value)-1]
		}
		loc, err := time.LoadLocation(dateConf[1])
		if err != nil {
			log.Printf("Error loading location: %v\n", err)
			continue
		}
		dateObj, err := time.Parse(dateConf[0], value)
		if err != nil {
			log.Printf("Error parsing date: %v\n", err)
			continue
		}
		destTz, err := time.LoadLocation(dateConf[2])
		if err != nil {
			destTz = time.UTC
		}
		dateObj = dateObj.In(loc).In(destTz)
		formattedPayload[source] = dateObj.Format(time.RFC3339)
	}
	return formattedPayload
}

// NullEmptyField sets null values for empty fields
func (adf *AbstractDataFormatter) NullEmptyField(formattedPayload map[string]interface{}) map[string]interface{} {
	for _, field := range adf.NullEmptyFieldMap {
		if value, exists := formattedPayload[field]; exists && (value == "" || value == "NULL") {
			formattedPayload[field] = nil
		}
	}
	return formattedPayload
}

// NullAllEmptyField sets null values for all empty fields
func (adf *AbstractDataFormatter) NullAllEmptyField(formattedPayload map[string]interface{}) map[string]interface{} {
	for field, value := range formattedPayload {
		if value == "" || value == "NULL" {
			formattedPayload[field] = nil
		}
	}
	return formattedPayload
}

// RemoveField removes specified fields from the payload
func (adf *AbstractDataFormatter) RemoveField(formattedPayload map[string]interface{}) map[string]interface{} {
	for _, field := range adf.RemoveFieldMap {
		delete(formattedPayload, field)
	}
	return formattedPayload
}

// NotDefinedField sets not defined fields based on NotDefinedField mapping
func (adf *AbstractDataFormatter) NotDefinedField(formattedPayload map[string]interface{}) map[string]interface{} {
	for code, label := range adf.NotDefinedFieldMap {
		if (formattedPayload[code] == "ERROR") || formattedPayload[code] == nil {
			formattedPayload[code] = adf.CodeNotDefined
			formattedPayload[label] = adf.LabelNotDefined
		}
	}
	return formattedPayload
}

// RenameField renames fields based on RenameField mapping
func (adf *AbstractDataFormatter) RenameField(formattedPayload map[string]interface{}) map[string]interface{} {
	for fromField, toField := range adf.RenameFieldMap {
		if value, exists := formattedPayload[fromField]; exists {
			formattedPayload[toField] = value
			delete(formattedPayload, fromField)
		}
	}
	return formattedPayload
}

// TrimField trims leading and trailing spaces or specified characters from all fields
func (adf *AbstractDataFormatter) TrimField(payload map[string]string, char *string) map[string]string {
	for key, data := range payload {
		if char != nil {
			payload[key] = strings.Trim(data, *char)
		} else {
			payload[key] = strings.TrimSpace(data)
		}
	}
	return payload
}
