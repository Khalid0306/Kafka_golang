package formatter

import (
	"strings"
	"time"
	"log"
)

// AbstractDataFormatter représente le formateur de données abstrait
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

// NewAbstractDataFormatter crée une nouvelle instance de AbstractDataFormatter
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

// FormatPath formate les chemins selon PathsMapping
func (adf *AbstractDataFormatter) FormatPath(payload map[string]string, formattedPayload map[string]interface{}) map[string]interface{} {
	for source, destination := range adf.PathsMapping {
		value := payload[source]
		formattedPayload[destination] = value
	}
	return formattedPayload
}

// FormatType formate les types selon TypesMapping
func (adf *AbstractDataFormatter) FormatType(formattedPayload map[string]interface{}) map[string]interface{} {
	for source, transform := range adf.TypesMapping {
		value := formattedPayload[source].(string)
		formattedPayload[source] = transform(value)
	}
	return formattedPayload
}

// FormatDate formate les dates selon DatesMapping
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
			log.Printf("Erreur de chargement de l'emplacement : %v\n", err)
			continue
		}
		dateObj, err := time.Parse(dateConf[0], value)
		if err != nil {
			log.Printf("Erreur de parsing de la date : %v\n", err)
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

// NullEmptyField définit des valeurs nulles pour les champs vides
func (adf *AbstractDataFormatter) NullEmptyField(formattedPayload map[string]interface{}) map[string]interface{} {
	for _, field := range adf.NullEmptyFieldMap {
		if value, exists := formattedPayload[field]; exists && (value == "" || value == "NULL") {
			formattedPayload[field] = nil
		}
	}
	return formattedPayload
}

// NullAllEmptyField définit des valeurs nulles pour tous les champs vides
func (adf *AbstractDataFormatter) NullAllEmptyField(formattedPayload map[string]interface{}) map[string]interface{} {
	for field, value := range formattedPayload {
		if value == "" || value == "NULL" {
			formattedPayload[field] = nil
		}
	}
	return formattedPayload
}

// RemoveField supprime les champs spécifiés du payload
func (adf *AbstractDataFormatter) RemoveField(formattedPayload map[string]interface{}) map[string]interface{} {
	for _, field := range adf.RemoveFieldMap {
		delete(formattedPayload, field)
	}
	return formattedPayload
}

// NotDefinedField définit les champs non définis selon NotDefinedFieldMapping
func (adf *AbstractDataFormatter) NotDefinedField(formattedPayload map[string]interface{}) map[string]interface{} {
	for code, label := range adf.NotDefinedFieldMap {
		if (formattedPayload[code] == "ERROR") || formattedPayload[code] == nil {
			formattedPayload[code] = adf.CodeNotDefined
			formattedPayload[label] = adf.LabelNotDefined
		}
	}
	return formattedPayload
}

// RenameField renomme les champs selon RenameFieldMapping
func (adf *AbstractDataFormatter) RenameField(formattedPayload map[string]interface{}) map[string]interface{} {
	for fromField, toField := range adf.RenameFieldMap {
		if value, exists := formattedPayload[fromField]; exists {
			formattedPayload[toField] = value
			delete(formattedPayload, fromField)
		}
	}
	return formattedPayload
}

// TrimField supprime les espaces ou les caractères spécifiés des champs
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

// TrimFieldInterface supprime les espaces des champs dans une map[string]interface{}
func (adf *AbstractDataFormatter) TrimFieldInterface(data map[string]interface{}, fields []string) map[string]interface{} {
	for key, value := range data {
		if strVal, ok := value.(string); ok {
			data[key] = strings.TrimSpace(strVal)
		}
	}
	return data
}
