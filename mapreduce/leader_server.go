package mapreduce

import (
	"encoding/json"
	"log"
	"net/http"
)

func Index(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode("Everything good")
}

func Server() {
	log.Println("Starting up server at port 8080")
	http.HandleFunc("/", Index)
	http.ListenAndServe(":8080", nil)
}
