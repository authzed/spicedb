tools=$(go list -f '{{range .Imports}}{{.}} {{end}}' tools.go)
go install $tools