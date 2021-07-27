package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/gomodule/redigo/redis"
)

type input struct {
	host, port, source, destination, how string
	daemonMode                           bool
	limit                                int
}

type how struct{ pop, push string }
type connection struct {
	redis            redis.Conn
	isLmoveSupported bool //When redis 6.2 releases
}

var conn = connection{}
var inputs = input{}

var howMap = map[string]how{
	"LTR": {"LPOP", "RPUSH"},
	"RTL": {"RPOP", "LPUSH"},
	"LTL": {"LPOP", "LPUSH"},
	"RTR": {"RPOP", "RPUSH"},
}

var pushCounter uint64 = 0

func main() {

	if err := inputs.getInputs(); err != nil {
		log.Println(err)
		return
	}

	if err := inputs.validateInputs(); err != nil {
		log.Println(err)
		return
	}

	if err := conn.createRedisConnection(inputs); err != nil {
		log.Printf("Could not create redis connection : %v", err)
		return
	}
	defer conn.redis.Close()

	if inputs.daemonMode {
		conn.processAsDaemon()
		log.Printf("Processed %d", pushCounter)
		return
	}

	conn.processAsScript()
	log.Printf("Processed %d", pushCounter)
}

//processAsDaemon runs till signal is received
func (c *connection) processAsDaemon() {

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	for {
		select {
		case s := <-sigs:
			log.Printf("Got signal : %v", s)
			return
		default:
			if err := c.process(&pushCounter, inputs); err != nil {
				log.Println(err)
			}
		}
	}
}

//processAsScript processes based on the queue length
func (c *connection) processAsScript() {

	qLength, err := conn.getQueueLength(inputs)
	if err != nil {
		log.Println(err)
		return
	}

	for i := 0; i < qLength; i++ {
		if err := c.process(&pushCounter, inputs); err != nil {
			log.Printf("Processed %d / %d", pushCounter, qLength)
			return
		}
	}
}

//validateInputs checks if data is proper
func (inputs *input) validateInputs() error {

	if _, ok := howMap[inputs.how]; !ok {
		return fmt.Errorf("how option is invalid")
	}

	log.Printf("Got Inputs : %+v", inputs)

	return nil
}

//getInputs get the inputs passed from shell
func (inputs *input) getInputs() error {

	flag.IntVar(&inputs.limit, "l", 0, "limit for queue length")
	flag.StringVar(&inputs.host, "h", "", "redis host")
	flag.StringVar(&inputs.port, "p", "", "redis port")
	flag.StringVar(&inputs.source, "s", "", "source queue name")
	flag.StringVar(&inputs.destination, "d", "", "destination queue name")
	flag.StringVar(&inputs.how, "t", "", "Type : LTR = Left to Right\nRTL = Right to Left\nLTL = Left to Left\nRTR = Right to Right")
	flag.BoolVar(&inputs.daemonMode, "daemon", false, "Use if you want to run as a daemon")
	help := flag.Bool("help", false, "-help")
	flag.Parse()

	if flag.NFlag() < 5 || *help {
		flag.PrintDefaults()
		return fmt.Errorf("All Flags are required")
	}

	inputs.how = strings.ToUpper(inputs.how)

	return nil
}

//createConnection creates redis connection and returns the conn struct
func (c *connection) createRedisConnection(inputs input) error {

	conn, err := redis.Dial("tcp", inputs.host+":"+inputs.port, redis.DialConnectTimeout(15*time.Second))

	if err != nil {
		return err
	}

	if _, err := conn.Do("CLIENT", "SETNAME", "redis-mqueue"); err != nil {
		return err
	}

	c.redis = conn

	return nil

}

//getQueueLength returns the source queue length
func (c *connection) getQueueLength(inputs input) (int, error) {

	if inputs.limit > 0 {
		return inputs.limit, nil
	}

	qLength, err := redis.Int(conn.redis.Do("LLEN", inputs.source))

	if err != nil {
		return 0, err
	}

	if qLength < 1 {
		return 0, fmt.Errorf("Source queue is empty")
	}

	return qLength, nil
}

//process pops from source and pushes to the destination
func (c *connection) process(pushCounter *uint64, inputs input) error {

	reply, err := c.redis.Do(howMap[inputs.how].pop, inputs.source)

	if reply == nil {
		return nil
	}

	popData, err := redis.Bytes(reply, err)

	if err != nil {
		log.Printf("Popped : %v", string(popData))
		log.Printf("Error occured while popping data , : %v", err)
		return err
	}

	if _, err := redis.Int(c.redis.Do(howMap[inputs.how].push, inputs.destination, popData)); err != nil {
		log.Printf("Error occured while pushing data, : %v", err)
		return err
	}

	*pushCounter++

	return nil

}
