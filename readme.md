## redis-mqueue

Simple tool to push data from source queue to destination queue.

Uses github.com/gomodule/redigo/redis for redis operations.

There are 5 mandatory options, 

 - h (redis host)
 - p (redis port)
 - s (source queue name)
 - d (destination queue name)
 - t (how)

Values for how

LTR = Left to Right  
RTL = Right to Left  
LTL = Left to Left  
RTR = Right to Right

**To install**, simply use `go get github.com/akshaykhairmode/redis-mqueue` 

This will install go binary in your $GOBIN (If its set) or at ~/go/bin/redis-mqueue 

Then you can run the below command to execute

Example : `$GOBIN/redis-mqueue -h="127.0.0.1" -p="6379" -s="source" -d="destination" -t=RTR`

Note : Will push the number of items based on the length of the queue when the tool is run. If want to run as daemon use --daemon option.
