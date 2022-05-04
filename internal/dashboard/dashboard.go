package dashboard

import (
	"fmt"
	"html/template"
	"net/http"
	"strings"

	"github.com/rs/zerolog/log"

	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/schemadsl/generator"
)

const rootTemplate = `
<html>
	<head>
		<link href="https://cdn.jsdelivr.net/npm/bootstrap@5.1.1/dist/css/bootstrap.min.css" rel="stylesheet" integrity="sha384-F3w7mX95PdgyTmZZMECAngseQB83DfGTowi0iMjiWaeVhAn4FJkqJByhZMI3AhiU" crossorigin="anonymous">
		<title>SpiceDB Dashboard</title>
		<style type="text/css">
		body {
			margin: 20px;
		}

		pre {
			border: 1px solid #ddd;
			background-color: #eee;
			padding: 10px;
		}
		</style>
		<!-- Global site tag (gtag.js) - Google Analytics -->
		<script async src="https://www.googletagmanager.com/gtag/js?id=G-7Z6F57MP7G"></script>
		<script>
		window.dataLayer = window.dataLayer || [];
		function gtag(){dataLayer.push(arguments);}
		gtag('js', new Date());

		gtag('config', 'G-7Z6F57MP7G');
		</script>
	</head>
	<body>
		{{if .IsReady }}
		{{if .IsEmpty}}
			<h1>Defining the permissions schema</h1>
			<p>
				To begin making API requests to SpiceDB, you'll first need to load in a <a href="https://docs.authzed.com/reference/schema-lang" target="_blank" rel="noopener">Schema</a>
				that defines the permissions system.
			</p>
			<p>
				Run the following command to load in a sample permissions system:

<pre>
# Install the zed CLI tool
brew install authzed/tap/zed

# Login to SpiceDB
zed context set first-dev-context {{ .GrpcAddr }} "the preshared key here" {{if not .GrpcTLSEnabled }}--insecure {{end}}

# Save the sample schema
cat > sample.zed << 'SCHEMA'
definition user {}

definition resource {
	relation reader: user
	relation writer: user

	permission write = writer
	permission view = reader + write
}
SCHEMA

# Write a sample schema
zed schema write sample.zed {{if not .GrpcTLSEnabled }}--insecure {{end}}
</pre>
			</p>
		{{ else }}
			<h1>SpiceDB</h1>
			<h2>Current Schema</h2>
			<pre>{{ .Schema }}</pre>

{{ if .HasSampleSchema }}
			<h2>Sample Calls</h2>
			<h3>How to write a relationship</h3>
<pre>
# Write a sample relationship
zed relationship create resource:sampleresource reader user:sampleuser {{if not .GrpcTLSEnabled }}--insecure {{end}}
</pre>

					<h3>How to check a permission</h3>
		<pre>
		# Check a permission
		zed permission check resource:sampleresource view user:sampleuser {{if not .GrpcTLSEnabled }}--insecure {{end}}
		</pre>
		{{ end }}
		{{ end }}
	{{ else }}
	<h1>Getting Started with SpiceDB</h1>
	<p>
		To get started with SpiceDB, please run the migrate command below to setup your backing data store:
	</p>
<pre>
spicedb migrate head --datastore-engine={{ .DatastoreEngine }} --datastore-conn-uri="your-connection-uri-here"
</pre>
	{{ end }}
	</body>
</html>
`

// NewHandler returns an http.Handler capable of serving a developer dashboard.
func NewHandler(grpcAddr string, grpcTLSEnabled bool, datastoreEngine string, ds datastore.Datastore) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tmpl, err := template.New("root").Parse(rootTemplate)
		if err != nil {
			log.Ctx(r.Context()).Error().AnErr("templateError", err).Msg("Got error when parsing template")
			fmt.Fprintf(w, "Internal Error")
			return
		}

		isReady, err := ds.IsReady(r.Context())
		if err != nil {
			log.Ctx(r.Context()).Error().AnErr("templateError", err).Msg("Got error when checking database")
			fmt.Fprintf(w, "Internal Error")
			return
		}

		schema := ""
		hasSampleSchema := false

		if isReady {
			var objectDefs []string
			userFound := false
			resourceFound := false

			headRevision, err := ds.HeadRevision(r.Context())
			if err != nil {
				log.Ctx(r.Context()).Error().Err(err).Msg("Got error when computing datastore revision")
				fmt.Fprintf(w, "Internal Error")
				return
			}

			nsDefs, err := ds.SnapshotReader(headRevision).ListNamespaces(r.Context())
			if err != nil {
				log.Ctx(r.Context()).Error().AnErr("datastoreError", err).Msg("Got error when trying to load namespaces")
				fmt.Fprintf(w, "Internal Error")
				return
			}

			for _, nsDef := range nsDefs {
				objectDef, _ := generator.GenerateSource(nsDef)
				objectDefs = append(objectDefs, objectDef)

				if nsDef.Name == "user" {
					userFound = true
				}
				if nsDef.Name == "resource" {
					resourceFound = true
				}
			}

			schema = strings.Join(objectDefs, "\n\n")
			hasSampleSchema = userFound && resourceFound
		}

		err = tmpl.Execute(w, struct {
			GrpcAddr        string
			GrpcTLSEnabled  bool
			DatastoreEngine string
			IsReady         bool
			IsEmpty         bool
			Schema          string
			HasSampleSchema bool
		}{
			GrpcAddr:        grpcAddr,
			GrpcTLSEnabled:  grpcTLSEnabled,
			DatastoreEngine: datastoreEngine,
			IsReady:         isReady,
			IsEmpty:         isReady && schema == "",
			Schema:          schema,
			HasSampleSchema: hasSampleSchema,
		})
		if err != nil {
			log.Ctx(r.Context()).Error().AnErr("templateError", err).Msg("Got error when executing template")
			fmt.Fprintf(w, "Internal Error")
			return
		}
	})
}
