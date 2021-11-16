package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const (
	// Timeout is the amount of time we'll wait for a response from the give Elasticsearch server.
	Timeout = 60 * time.Second
)

var (
	// ErrNoMorePages is an error returned when no more pages can be fetched.
	ErrNoMorePages = errors.New("no more pages to fetch")
)

// Result represents the parts of an Elasticsearch result set that we need.
type Result struct {
	ScrollID string     `json:"_scroll_id"`
	Hits     ResultHits `json:"hits"`
}

// ResultHits represents the hits section of an Elasticsearch result set.
type ResultHits struct {
	Total ResultsTotal `json:"total"`
	Hits  []ResultHit  `json:"hits"`
}

// ResultsTotal describes the total hits produced by a query.
type ResultsTotal struct {
	Value    uint   `json:"value"`
	Relation string `json:"relation"`
}

// ResultHit is an individual result hit, found in a set of ResultHits.
type ResultHit struct {
	Source map[string]interface{} `json:"_source"`
}

func main() {
	var host string
	var rawQuery string
	var query string
	var index string
	var fields string

	flag.StringVar(&host, "host", "", "The Elasticsearch host, e.g. 'http://localhost:9200'")
	flag.StringVar(&rawQuery, "raw-query", "", "An Elasticsearch JSON query object")
	flag.StringVar(&query, "query", "", "A Lucene-syntax search query")
	flag.StringVar(&fields, "fields", "", "A comma separated list of fields to include")
	flag.StringVar(&index, "index", "", "An [optional] index to search within")
	flag.Parse()

	if host == "" {
		fatal(errors.New("host must be set"))
	}

	if rawQuery == "" && query == "" {
		fatal(errors.New("raw-query or query must be set"))
	}

	if fields == "" {
		fatal(errors.New("fields must be set"))
	}

	hostURL, err := url.Parse(host)
	if err != nil {
		fatal(fmt.Errorf("failed to parse host: %s: %v", hostURL, err))
	}

	filter := strings.Split(fields, ",")

	var scrollID string
	var header []string
	var hitCount int

	for {
		result, err := getPage(scrollID, hostURL.String(), index, rawQuery, query)
		if err != nil && err != ErrNoMorePages {
			fatal(err)
		}

		hits := result.Hits.Hits
		hitCount += len(hits)

		// If we're on the first page, let's write the CSV header
		if scrollID == "" {
			header = getHeader(hits[0], filter)
			fmt.Println(strings.Join(header, ","))
		}

		// Print the rest of the results with the fields ordered by the header.
		for _, h := range hits {
			var cells []string

			for _, k := range header {
				var cell string

				v, ok := h.Source[k]
				if ok {
					cell = fmt.Sprintf("%v", v)
				}

				cells = append(cells, cell)
			}

			fmt.Println(strings.Join(cells, ","))
		}

		scrollID = result.ScrollID

		log.Printf("got %d of %d", hitCount, result.Hits.Total.Value)

		if len(hits) == int(result.Hits.Total.Value) {
			break
		}

		if err == ErrNoMorePages {
			break
		}
	}
}

// getPage returns the next page of results.
func getPage(scrollID, host, index, rawQuery, query string) (Result, error) {
	if scrollID == "" {
		return getFirstPage(host, index, rawQuery, query)
	}

	searchURL := fmt.Sprintf("%s/_search/scroll", host)
	reqBody := strings.NewReader(fmt.Sprintf(`
		{
			"scroll": "2m",
			"scroll_id": "%s"
		}
	`, scrollID))

	req, err := http.NewRequest("POST", searchURL, reqBody)
	if err != nil {
		return Result{}, err
	}

	return sendAndDecode(req)
}

// getFirstPage gets the first page of results.
func getFirstPage(host, index, rawQuery, query string) (Result, error) {
	searchURL := fmt.Sprintf("%s/_search", host)
	if index != "" {
		searchURL = fmt.Sprintf("%s/%s/_search?scroll=2m", host, index)
	}

	var reqBody string

	if rawQuery != "" {
		reqBody = rawQuery
	} else if query != "" {
		reqBody = fmt.Sprintf(`
			{
				"size": 10000,
				"query": {
					"query_string": {
						"query": "%s"
					}
				}
			}
		`, query)
	}

	req, err := http.NewRequest("POST", searchURL, strings.NewReader(reqBody))
	if err != nil {
		return Result{}, err
	}

	return sendAndDecode(req)
}

// sendAndDecode sends the given request and decodes the resulting response body.
func sendAndDecode(req *http.Request) (Result, error) {
	ctx, cfn := context.WithTimeout(context.Background(), Timeout)
	defer cfn()

	req = req.WithContext(ctx)
	req.Header.Add("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return Result{}, err
	}

	defer resp.Body.Close()

	var result Result

	err = json.NewDecoder(resp.Body).Decode(&result)
	if err != nil {
		return Result{}, err
	}

	if len(result.Hits.Hits) == 0 {
		return Result{}, ErrNoMorePages
	}

	return result, nil
}

// fatal will print the given error, wrapped, if it is not nil, and then exit the program with an
// exit code 1.
func fatal(err error) {
	if err != nil {
		log.Fatalf("fatal: %v\n", err)
	}
}

// getHeader gets the header strings for the results. The given fields will be used to filter the
// keys on the given ResultHit it's not an empty slice of strings.
func getHeader(hit ResultHit, fields []string) []string {
	var header []string

	for k := range hit.Source {
		if len(fields) != 0 && !stringSliceContains(fields, k) {
			continue
		}

		header = append(header, k)
	}

	return header
}

// stringSliceContains returns true if the given string slice contains the given string.
func stringSliceContains(strs []string, str string) bool {
	for _, s := range strs {
		if s == str {
			return true
		}
	}

	return false
}
