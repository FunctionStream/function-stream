{{/*
Expand the name of the chart.
*/}}
{{- define "function-mesh-operator.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}