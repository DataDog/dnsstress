package main

import (
	"fmt"
	"github.com/miekg/dns"
)

type Request struct {
	server         string
	domain 		   string
}

func (r *Request) exchange () (int64, error) {
	msg := new(dns.Msg).SetQuestion(r.domain, dns.TypeA)
	udpConn, err := dns.Dial("udp", r.server)
	if err != nil {
		return 0, err
	}
	defer udpConn.Close()
	err = udpConn.WriteMsg(msg)
	if err != nil {
		fmt.Println(err)
		return 0, err
	}

	return int64(msg.Len()), nil
}

