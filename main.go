package main

import (
	"bufio"
	"flag"
	"log"
	"net"
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/zobo/mrproxy/protocol"
	"github.com/zobo/mrproxy/proxy"
	"github.com/zobo/mrproxy/stats"
)

var version string

// "10.13.37.106:6379"
var redis_server = flag.String("server", "127.0.0.1:6379", "Redis server to connect to")
var listen_addr = flag.String("bind", "0.0.0.0:11211", "Bind address and port")

func main() {

	flag.Parse()

	// move to global??
	pool := &redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			//c, err := redis.Dial("tcp", *redis_server)
			d, _ := time.ParseDuration("1s")
			c, err := redis.DialTimeout("tcp", *redis_server, d, d, d)
			if err != nil {
				return nil, err
			}
			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}

	l, err := net.Listen("tcp", *listen_addr)
	if err != nil {
		log.Fatalln(err)
	}
	log.Printf("Listening %v", l)
	for {
		c, err := l.Accept()
		if err != nil {
			log.Println(err)
		}
		go processMc(c, pool)
	}
}

func processMc(c net.Conn, pool *redis.Pool) {
	defer c.Close()

	stats.Connect()
	defer stats.Disconnect()

	// process
	br := bufio.NewReader(c)
	bw := bufio.NewWriter(c)

	// take it per need
	conn := pool.Get()
	defer conn.Close()

	redisProxy := proxy.NewRedisProxy(conn)
	proxy := stats.NewStatsProxy(redisProxy)

	for {
		req, err := protocol.ReadRequest(br)
		if perr, ok := err.(protocol.ProtocolError); ok {
			log.Printf("%v ReadRequest protocol err: %v", c, err)
			bw.WriteString("CLIENT_ERROR " + perr.Error() + "\r\n")
			bw.Flush()
			continue
		} else if err != nil {
			return
		}
		switch req.Command {
		case "quit":
			return
		case "version":
			res := protocol.McResponse{Response: "VERSION mrproxy " + version}
			bw.WriteString(res.Protocol())
			bw.Flush()
		default:
			res := proxy.Process(req)
			if !req.Noreply {
				bw.WriteString(res.Protocol())
				bw.Flush()
			}
			if res.Response == "SERVER_ERROR" {
				log.Println(res)
				return
			}
		}
	}
}
