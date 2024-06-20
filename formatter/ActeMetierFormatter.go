package formatter

import (
	"fmt"
	"log"
	"strings"
)

type ActeMetierFormatter struct {
	*AbstractCsvFormatter
	Formatter *StandardFormatter
	AbstractDataFormatter *AbstractDataFormatter
}

func NewActeMetierFormatter(formatter *StandardFormatter, logger *log.Logger) *ActeMetierFormatter {
	return &ActeMetierFormatter{
		AbstractCsvFormatter: NewAbstractCsvFormatter(logger),
		Formatter:            formatter,
	}
}

func (amf *ActeMetierFormatter) GetRow(data map[string]interface{}) map[string]interface{} {
	// Convert map[string]interface{} to map[string]string
	dataString := convertMapStringInterfaceToStringString(data)
	dataString = amf.AbstractDataFormatter.TrimField(dataString, nil)

	// Convert map[string]string back to map[string]interface{}
	data = convertMapStringStringToInterface(dataString)

	if data["CodeFamilleActe"] == "ERROR" {
		data["CodeFamilleActe"] = nil
		data["LibelleFamilleActe"] = nil
	}

	if codeTypologieActe, exists := data["CodeTypologieActe"]; exists && codeTypologieActe == "ERROR" {
		data["CodeTypologieActe"] = nil
		data["LibelleTypologieActe"] = nil
	}

	if data["CodeEtatActe"] == "ERROR" {
		data["CodeEtatActe"] = "Enc"
		data["LibelleEtatActe"] = "A réaliser"
	}

	if _, exists := data["DateFinActeMetier"]; exists {
		data["CodeEtatActe"] = "Rea"
		data["LibelleEtatActe"] = "Réalisé"
	}

	if _, exists := data["SystemeExterne"]; !exists {
		data["SystemeExterne"] = "Manuelle"
	}

	if codeSite, exists := data["CodeSite"]; exists {
		data["SiteVisibleId"] = fmt.Sprintf("MDM-%v", codeSite)
	}

	if data["ApplicationSource"] == "OutillageNeptune" && data["CodeStandardActe"] == "S01" && data["LibelleStandardActe"] == nil {
		data["LibelleStandardActe"] = "Intervention Usine"
	}

	applicationInstanceString := ""
	if applicationInstance, exists := data["ApplicationInstance"]; exists {
		applicationInstanceString = fmt.Sprintf("%v-", applicationInstance)
	}

	data["IdActeMetier"] = data["NumeroActeMetier"]
	data["IdIntervention"] = data["NumeroIntervention"]

	data["InterventionId"] = fmt.Sprintf("%v-%v%v", data["ApplicationSource"], applicationInstanceString, data["NumeroIntervention"])
	data["Id"] = fmt.Sprintf("%v-%v", data["InterventionId"], data["NumeroActeMetier"])

	data["VdmSource"] = "SAS Data"
	data["VdmProducer"] = "Suez"
	data["VdmCategorie"] = "Intervention"

	if intervenant, exists := data["Intervenant"]; exists {
		switch intervenant {
		case "Interne (Drone Volant)", "Interne (Drone Bateau)", "Interne":
			data["Intervenant"] = "INTERNE"
		case "Externe":
			data["Intervenant"] = "EXTERNE"
		}
		if strings.HasPrefix(intervenant.(string), "Erreur") || intervenant == "" {
			data["Intervenant"] = nil
		}
	}

	// Convert map[string]string to map[string]interface{}
    formattedData := amf.Formatter.Format(convertMapStringStringToInterface(dataString))
    returnData := make(map[string]interface{})
    for key, value := range formattedData {
        returnData[key] = value
    }
    return returnData
}

// Helper function to convert map[string]interface{} to map[string]string
func convertMapStringInterfaceToStringString(data map[string]interface{}) map[string]string {
    newData := make(map[string]string)
    for key, value := range data {
        newData[key] = fmt.Sprintf("%v", value)
    }
    return newData
}

// Helper function to convert map[string]string to map[string]interface{}
func convertMapStringStringToInterface(data map[string]string) map[string]interface{} {
    newData := make(map[string]interface{})
    for key, value := range data {
        newData[key] = value
    }
    return newData
}
