mrcoordinator:
	go run -race coordinator/mrcoordinator.go map_data/pg*.txt

wc:
	go build --buildmode=plugin wc.go   
id:
	go build --buildmode=plugin indexer.go   