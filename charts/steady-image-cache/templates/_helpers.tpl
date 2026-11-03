{{/*
Expand the name of the chart.
*/}}
{{- define "steady-image-cache.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "steady-image-cache.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "steady-image-cache.labels" -}}
helm.sh/chart: {{ include "steady-image-cache.chart" . }}
{{ include "steady-image-cache.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "steady-image-cache.selectorLabels" -}}
app.kubernetes.io/name: {{ include "steady-image-cache.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app: {{ include "steady-image-cache.name" . }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "steady-image-cache.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
ServiceAccount name
*/}}
{{- define "steady-image-cache.serviceAccountName" -}}
{{- default (include "steady-image-cache.fullname" .) .Values.serviceAccount.name }}
{{- end }}

{{/*
Port of the plain-HTTP admin listener (probes, metrics), taken from config.adminListen.
*/}}
{{- define "steady-image-cache.adminPort" -}}
{{- last (splitList ":" .Values.config.adminListen) -}}
{{- end }}

{{/*
True when config.adminListen only accepts connections from the node itself.
*/}}
{{- define "steady-image-cache.adminIsLoopback" -}}
{{- if or (hasPrefix "127." .Values.config.adminListen) (hasPrefix "localhost:" .Values.config.adminListen) (hasPrefix "[::1]:" .Values.config.adminListen) -}}true{{- end -}}
{{- end }}

{{/*
Host the probes connect to: the address config.adminListen binds, or loopback
when it binds every address (or only loopback).
*/}}
{{- define "steady-image-cache.adminHost" -}}
{{- $l := .Values.config.adminListen -}}
{{- $host := trimSuffix (printf ":%s" (include "steady-image-cache.adminPort" .)) $l | trimPrefix "[" | trimSuffix "]" -}}
{{- if or (eq $host "") (eq $host "0.0.0.0") -}}127.0.0.1
{{- else if eq $host "::" -}}::1
{{- else -}}{{ $host }}
{{- end -}}
{{- end }}
