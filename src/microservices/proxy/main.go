package main

import (
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strconv"
	"strings"
)

var (
	monolithURL      *url.URL
	moviesServiceURL *url.URL
	eventsServiceURL *url.URL
	gradualMigration bool
	migrationPercent int
)

func main() {
	initConfig()

	http.HandleFunc("/health", healthHandler)
	http.HandleFunc("/", proxyHandler)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8000"
	}
	log.Printf("Starting proxy service on port %s", port)
	log.Printf("Gradual migration: %v, migration percent: %d%%", gradualMigration, migrationPercent)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}

func initConfig() {
	var err error

	monolithRaw := os.Getenv("MONOLITH_URL")
	if monolithRaw == "" {
		monolithRaw = "http://localhost:8080"
	}
	monolithURL, err = url.Parse(monolithRaw)
	if err != nil {
		log.Fatalf("Invalid MONOLITH_URL: %v", err)
	}

	moviesRaw := os.Getenv("MOVIES_SERVICE_URL")
	if moviesRaw == "" {
		moviesRaw = "http://localhost:8081"
	}
	moviesServiceURL, err = url.Parse(moviesRaw)
	if err != nil {
		log.Fatalf("Invalid MOVIES_SERVICE_URL: %v", err)
	}

	eventsRaw := os.Getenv("EVENTS_SERVICE_URL")
	if eventsRaw == "" {
		eventsRaw = "http://localhost:8082"
	}
	eventsServiceURL, err = url.Parse(eventsRaw)
	if err != nil {
		log.Fatalf("Invalid EVENTS_SERVICE_URL: %v", err)
	}

	gradualMigration, _ = strconv.ParseBool(os.Getenv("GRADUAL_MIGRATION"))
	migrationPercent, _ = strconv.Atoi(os.Getenv("MOVIES_MIGRATION_PERCENT"))
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	fmt.Fprint(w, "Strangler Fig Proxy is healthy")
}

func proxyHandler(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path

	switch {
	case strings.HasPrefix(path, "/api/events"):
		proxyTo(eventsServiceURL, w, r)
	case strings.HasPrefix(path, "/api/movies"):
		target := resolveMoviesTarget()
		proxyTo(target, w, r)
	default:
		proxyTo(monolithURL, w, r)
	}
}

func resolveMoviesTarget() *url.URL {
	if !gradualMigration {
		return moviesServiceURL
	}
	if rand.Intn(100) < migrationPercent {
		log.Println("Routing /api/movies -> movies-service")
		return moviesServiceURL
	}
	log.Println("Routing /api/movies -> monolith")
	return monolithURL
}

func proxyTo(target *url.URL, w http.ResponseWriter, r *http.Request) {
	proxy := httputil.NewSingleHostReverseProxy(target)
	proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		log.Printf("Proxy error: %v", err)
		http.Error(w, "Service unavailable", http.StatusBadGateway)
	}
	r.Host = target.Host
	proxy.ServeHTTP(w, r)
}
