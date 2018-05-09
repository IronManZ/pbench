# pbench
A java based HTTP request load generator

## Build and install
```bash 
mvn install
```
## Usage
```bash
java -jar target/pbench-1.0-SNAPSHOT.jar -t 10 -l INFO -b 1 -q 300 -i request_sample_5.csv "http://example.com"
```

## Arguments Specification:
* -t: test time in seconds
* -v: view response content
* -q: QPS
* -g: the number of seconds in a group, all the request in a group will be distributed evenly into small windows and sent
out at the beginning of each window.
* -b: number of batches/windows per group,
* -l: print info every N loops