package main

import (
	"flag"
	"log"
	"os"
	"time"

	"github.com/gomodule/redigo/redis"
)

type input struct{ host, port, source, destination, how string }
type how struct{ pop, push string }

var inputs = input{}

var howMap = map[string]how{
	"LTR": {"LPOP", "RPUSH"},
	"RTL": {"RPOP", "LPUSH"},
	"LTL": {"LPOP", "LPUSH"},
	"RTR": {"RPOP", "RPUSH"},
}

func main() {

	flag.StringVar(&inputs.host, "host", "", "redis host")
	flag.StringVar(&inputs.port, "port", "", "redis port")
	flag.StringVar(&inputs.source, "source", "", "source queue name")
	flag.StringVar(&inputs.destination, "destination", "", "destination queue name")
	flag.StringVar(&inputs.how, "how", "", "LTR = Left to Right\nRTL = Right to Left\nLTL = Left to Left\nRTR = Right to Right")
	help := flag.Bool("help", false, "-help")
	flag.Parse()

	if flag.NFlag() < 5 || *help {
		log.Println("All Flags are required.")
		flag.PrintDefaults()
		os.Exit(0)
	}

	defer log.Println("Exiting")

	log.Printf("Got Inputs : %+v", inputs)

	conn, err := redis.Dial("tcp", inputs.host+":"+inputs.port, redis.DialConnectTimeout(15*time.Second))
	defer conn.Close()

	if err != nil {
		log.Printf("Could not connect to redis : %v", err)
		return
	}

	qLength, err := redis.Int(conn.Do("LLEN", inputs.source))

	if err != nil {
		log.Printf("Error occured while getting queue length : %v", err)
		return
	}

	if qLength < 1 {
		log.Println("Source queue is empty")
		return
	}

	pushCounter := 0

	for i := 0; i < qLength; i++ {
		if err := process(conn, &pushCounter); err != nil {
			return
		}
	}

	log.Printf("Processed %d / %d", pushCounter, qLength)

}

//process pops from source and pushes to the destination
func process(conn redis.Conn, pushCounter *int) error {

	popData, err := redis.Bytes(conn.Do(howMap[inputs.how].pop, inputs.source))

	if err != nil {
		log.Printf("Popped : %v", string(popData))
		log.Printf("Error occured while popping data , : %v", err)
		return err
	}

	if _, err := redis.Int(conn.Do(howMap[inputs.how].push, inputs.destination, popData)); err != nil {
		log.Printf("Error occured while pushing data, : %v", err)
		return err
	}

	*pushCounter++

	return nil

}
