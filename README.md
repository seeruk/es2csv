# es2csv

Elasticsearch to CSV exporter, written in Go.

## Installation

You'll need Go installed, then simply run:

```
$ go install github.com/seeruk/es2csv@latest
```

## Usage

`es2csv` is very easy to use. CSV output will go to stdout, any log output goes to stderr.

```
Usage of es2csv:
  -fields string
        A comma separated list of fields to include
  -host string
        The Elasticsearch host, e.g. 'http://localhost:9200'
  -index string
        An [optional] index to search within
  -query string
        A Lucene-syntax search query
  -raw-query string
        An Elasticsearch JSON query object
```

Example:

```
./es2csv \
    -host "https://elasticsearch-prod-abc123.eu-west-1.es.amazonaws.com" \ 
    -index "fluentd-*" \
    -fields "error,code,request_id" \
    -query "@log_name:auth.api AND _exists_:error"
```

## License

MIT
