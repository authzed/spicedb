package v0

import (
	"net/http"
	"strings"

	log "github.com/authzed/spicedb/internal/logging"

	"gopkg.in/yaml.v3"
)

const downloadPath = "/download/"

func DownloadHandler(shareStore ShareStore) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc(downloadPath, downloadHandler(shareStore))
	return mux
}

func downloadHandler(shareStore ShareStore) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		ref := strings.TrimPrefix(r.URL.Path, downloadPath)
		if ref == "" {
			http.Error(w, "ref is missing", http.StatusBadRequest)
			return
		}
		if strings.Contains(ref, "/") {
			http.Error(w, "ref may not contain a '/'", http.StatusBadRequest)
			return
		}
		shared, status, err := shareStore.LookupSharedByReference(ref)
		if err != nil {
			log.Ctx(r.Context()).Debug().Str("id", ref).Err(err).Msg("Lookup Shared Error")
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		if status == LookupNotFound {
			log.Ctx(r.Context()).Debug().Str("id", ref).Msg("Lookup Shared Not Found")
			http.NotFound(w, r)
			return
		}
		out, err := yaml.Marshal(&shared)
		if err != nil {
			log.Ctx(r.Context()).Debug().Str("id", ref).Err(err).Msg("Couldn't marshall as yaml")
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if _, err := w.Write(out); err != nil {
			log.Ctx(r.Context()).Debug().Str("id", ref).Err(err).Msg("Couldn't write as yaml")
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
	}
}
