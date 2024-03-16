package mapreduce

import (
	"encoding/json"
	"net/http"

	"github.com/rs/zerolog/log"
)

func Index(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode("Everything good")
}

func Server() {
	log.Info().Msg("Starting up server at port 8080")
	http.HandleFunc("/", Index)
	http.ListenAndServe(":8080", nil)
}
