package formatter

import (
	"strconv"
)

type StandardFormatter struct {
	*AbstractDataFormatter
}

// NewStandardFormatter creer une nouvelle instance de StandardFormatter
func NewStandardFormatter() *StandardFormatter {
	sf := &StandardFormatter{
		AbstractDataFormatter: NewAbstractDataFormatter(),
	}
	sf.RemoveFieldMap = []string{
		"sysDateSourceCreation",
		"sysDateSourceMiseAJour",
		"sysUserCreation",
		"sysDateCreation",
		"sysUserMiseAJour",
		"sysDateMiseAJour",
	}

	sf.NullEmptyFieldMap = []string{
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
	}

	sf.DatesMapping = map[string][3]string{
		"DateDebutActeMetier": {"2006-01-02 15:04:05.000000", "Europe/Paris", "UTC"},
		"DateFinActeMetier":   {"2006-01-02 15:04:05.000000", "Europe/Paris", "UTC"},
	}

	sf.TypesMapping = map[string]func(string) interface{}{
		"NombreActe": func(val string) interface{} {
			i, err := strconv.Atoi(val)
			if err != nil {
				return nil
			}
			return i
		},
	}

	return sf
}

// Formater le payload
func (sf *StandardFormatter) Format(payload map[string]interface{}) map[string]interface{} {
	payload = sf.FormatDate(payload)
	payload = sf.FormatType(payload)
	payload = sf.NullEmptyField(payload)
	payload = sf.RemoveField(payload)
	return payload
}
