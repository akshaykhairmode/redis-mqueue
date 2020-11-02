## redis-mqueue

Simple tool to push data from source queue to destination queue.

Uses github.com/gomodule/redigo/redis for redis operations.

There are 5 mandatory options, 

 - host
 - port
 - source
 - destination
 - how

Values for how

LTR = Left to Right  
RTL = Right to Left  
LTL = Left to Left  
RTR = Right to Right

Example : `go run main.go -host="127.0.0.1" -port="6379" -source="source" -destination="destination" -how=RTR`

Note : Will push the number of items based on the length of the queue when the tool is run.
