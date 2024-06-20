package formatter

import (
	"fmt"
	"log"
	"strings"
)

type ActeMetierFormatter struct {
	*formatter.AbstractCsvFormatter
	Formatter *formatter.StandardFormatter
}

func NewActeMetierFormatter(formatter *formatter.StandardFormatter, logger *log.Logger) *ActeMetierFormatter {
	return &ActeMetierFormatter{
		AbstractCsvFormatter: formatter.NewAbstractCsvFormatter(logger),
		Formatter:            formatter,
	}
}

func (amf *ActeMetierFormatter) GetRow(data map[string]interface{}) map[string]interface{} {
	data = amf.Formatter.trimField(data)
	data = amf.Formatter.nullEmptyField(data)

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

	return amf.Formatter.Format(data)
}

