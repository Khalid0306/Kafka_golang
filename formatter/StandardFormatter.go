package formatter

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

type AbstractDataFormatter interface {
	Format(payload map[string]interface{}) map[string]interface{}
}

type StandardFormatter struct {
	RemoveField    []string
	NullEmptyField []string
	DatesMapping   map[string][]string
	TypesMapping   map[string]func(string) (interface{}, error)
}

func NewStandardFormatter() *StandardFormatter {
	return &StandardFormatter{
		RemoveField: []string{
			"sysDateSourceCreation",
			"sysDateSourceMiseAJour",
			"sysUserCreation",
			"sysDateCreation",
			"sysUserMiseAJour",
			"sysDateMiseAJour",
		},
		NullEmptyField: []string{
			"DateDebutActeMetier",
			"DateFinActeMetier",
			"CodeSite",
			"IdentifiantVICR",
			"ApplicationInstance",
			"SystemeExterne",
			"CodeStandardActe",
			"LibelleStandardActe",
			"CodeActe",
			"LibelleActe",
			"CodeFamilleActe",
			"LibelleFamilleActe",
			"CodeEtatActe",
			"LibelleEtatActe",
			"CodeTypologieActe",
			"LibelleTypologieActe",
		},
		DatesMapping: map[string][]string{
			"DateDebutActeMetier": {"2006-01-02 15:04:05.999999", "Europe/Paris", "UTC"},
			"DateFinActeMetier":   {"2006-01-02 15:04:05.999999", "Europe/Paris", "UTC"},
		},
		TypesMapping: map[string]func(string) (interface{}, error){
			"NombreActe": func(value string) (interface{}, error) {
				return strconv.Atoi(value)
			},
		},
	}
}

func (sf *StandardFormatter) formatDate(payload map[string]interface{}) map[string]interface{} {
	for field, dateConf := range sf.DatesMapping {
		if value, exists := payload[field]; exists && value != nil {
			strValue := value.(string)
			if strings.Contains(strValue, ".") && len(strings.Split(strValue, ".")[1]) == 7 {
				strValue = strValue[:len(strValue)-1]
			}

			loc, err := time.LoadLocation(dateConf[1])
			if err != nil {
				loc = time.UTC
			}
			dateObj, err := time.ParseInLocation(dateConf[0], strValue, loc)
			if err == nil {
				destLoc, err := time.LoadLocation(dateConf[2])
				if err != nil {
					destLoc = time.UTC
				}
				payload[field] = dateObj.In(destLoc).Format(time.RFC3339)
			}
		}
	}
	return payload
}

func (sf *StandardFormatter) formatType(payload map[string]interface{}) map[string]interface{} {
	for field, transform := range sf.TypesMapping {
		if value, exists := payload[field]; exists && value != nil {
			strValue := value.(string)
			convertedValue, err := transform(strValue)
			if err == nil {
				payload[field] = convertedValue
			}
		}
	}
	return payload
}

func (sf *StandardFormatter) nullEmptyField(payload map[string]interface{}) map[string]interface{} {
	for _, field := range sf.NullEmptyField {
		if value, exists := payload[field]; exists && (value == nil || value == "" || value == "NULL") {
			payload[field] = nil
		}
	}
	return payload
}

func (sf *StandardFormatter) removeField(payload map[string]interface{}) map[string]interface{} {
	for _, field := range sf.RemoveField {
		delete(payload, field)
	}
	return payload
}

func (sf *StandardFormatter) Format(payload map[string]interface{}) map[string]interface{} {
	payload = sf.formatDate(payload)
	payload = sf.formatType(payload)
	payload = sf.nullEmptyField(payload)
	payload = sf.removeField(payload)

	return payload
}

