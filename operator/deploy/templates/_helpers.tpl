{{/*
Expand the name of the service account to use
*/}}
{{- define "functionstream-operator.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- if .Values.serviceAccount.name }}
{{ .Values.serviceAccount.name }}
{{- else -}}
{{ include "functionstream-operator.fullname" . }}
{{- end -}}
{{- else -}}
{{ .Values.serviceAccount.name }}
{{- end -}}
{{- end -}}

{{/*
Expand the fullname of the chart
*/}}
{{- define "functionstream-operator.fullname" -}}
{{- printf "%s-%s" .Release.Name "operator" | trunc 63 | trimSuffix "-" -}}
{{- end -}} 