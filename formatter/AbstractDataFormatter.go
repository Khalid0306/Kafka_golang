package formatter

import (
	"strings"
	"time"
)

type AbstractDataFormatter interface {
	Format(payload map[string]interface{}) map[string]interface{}
}

type BaseFormatter struct {
	PATHS_MAPPING     map[string]string
	TYPES_MAPPING     map[string]func(interface{}) interface{}
	DATES_MAPPING     map[string][]string
	NULL_EMPTY_FIELD  []string
	REMOVE_FIELD      []string
	NOT_DEFINED_FIELD map[string]string
	RENAME_FIELD      map[string]string

	CODE_NOT_DEFINED  string
	LABEL_NOT_DEFINED string
}

func NewBaseFormatter() *BaseFormatter {
	return &BaseFormatter{
		PATHS_MAPPING:     make(map[string]string),
		TYPES_MAPPING:     make(map[string]func(interface{}) interface{}),
		DATES_MAPPING:     make(map[string][]string),
		NULL_EMPTY_FIELD:  []string{},
		REMOVE_FIELD:      []string{},
		NOT_DEFINED_FIELD: make(map[string]string),
		RENAME_FIELD:      make(map[string]string),

		CODE_NOT_DEFINED:  "NonDefini",
		LABEL_NOT_DEFINED: "Non défini",
	}
}

func (bf *BaseFormatter) FormatPath(payload, formattedPayload map[string]interface{}) map[string]interface{} {
	for source, destination := range bf.PATHS_MAPPING {
		if value, exists := payload[source]; exists {
			formattedPayload[destination] = value
		}
	}
	return formattedPayload
}

func (bf *BaseFormatter) FormatType(formattedPayload map[string]interface{}) map[string]interface{} {
	for source, transform := range bf.TYPES_MAPPING {
		if value, exists := formattedPayload[source]; exists {
			formattedPayload[source] = transform(value)
		}
	}
	return formattedPayload
}

func (bf *BaseFormatter) FormatDate(formattedPayload map[string]interface{}) map[string]interface{} {
	for source, dateConf := range bf.DATES_MAPPING {
		if value, exists := formattedPayload[source]; exists {
			if strValue, ok := value.(string); ok && strValue != "" {
				if strings.Contains(strValue, ".") && len(strings.Split(strValue, ".")[1]) == 7 {
					strValue = strValue[:len(strValue)-1]
				}
				dateObj, err := time.Parse(dateConf[0], strValue)
				if err == nil {
					destTz, _ := time.LoadLocation(dateConf[2])
					dateObj = dateObj.In(destTz)
					formattedPayload[source] = dateObj.Format(time.RFC3339)
				}
			}
		}
	}
	return formattedPayload
}

func (bf *BaseFormatter) NullEmptyField(formattedPayload map[string]interface{}) map[string]interface{} {
	for _, field := range bf.NULL_EMPTY_FIELD {
		if value, exists := formattedPayload[field]; exists && (value == nil || value == "NULL" || value == "") {
			formattedPayload[field] = nil
		}
	}
	return formattedPayload
}

func (bf *BaseFormatter) NullAllEmptyField(formattedPayload map[string]interface{}) map[string]interface{} {
	for field, value := range formattedPayload {
		if value == nil || value == "NULL" || value == "" {
			formattedPayload[field] = nil
		}
	}
	return formattedPayload
}

func (bf *BaseFormatter) RemoveField(formattedPayload map[string]interface{}) map[string]interface{} {
	for _, field := range bf.REMOVE_FIELD {
		delete(formattedPayload, field)
	}
	return formattedPayload
}

func (bf *BaseFormatter) NotDefinedField(formattedPayload map[string]interface{}) map[string]interface{} {
	for code, label := range bf.NOT_DEFINED_FIELD {
		if value, exists := formattedPayload[code]; !exists || value == "ERROR" || value == "" {
			formattedPayload[code] = bf.CODE_NOT_DEFINED
			formattedPayload[label] = bf.LABEL_NOT_DEFINED
		}
	}
	return formattedPayload
}

func (bf *BaseFormatter) RenameField(formattedPayload map[string]interface{}) map[string]interface{} {
	for fromField, toField := range bf.RENAME_FIELD {
		if value, exists := formattedPayload[fromField]; exists {
			formattedPayload[toField] = value
			delete(formattedPayload, fromField)
		}
	}
	return formattedPayload
}

func (bf *BaseFormatter) TrimField(payload map[string]interface{}, char string) map[string]interface{} {
	for key, data := range payload {
		if strData, ok := data.(string); ok {
			if char != "" {
				payload[key] = strings.Trim(strData, char)
			} else {
				payload[key] = strings.TrimSpace(strData)
			}
		}
	}
	return payload
}

