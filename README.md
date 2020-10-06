# DNSStress, the DNS stress test tool

Simple Go program to stress test a DNS server.

It displays the number of queries made, along with the answer per second rate reached.

## Usage

First:

    go get github.com/DataDog/dnsstress

(Credit to original writer [Mickael Bergem](https://github.com/MickaelBergem/dnsstresss))

Then:

    $ dnsstress -h
    dnsstress - dns stress tool

    Send DNS requests as fast as possible to a given server and display the rate.

    Usage: dnsstress [option ...] targetdomain [targetdomain [...] ]
    -concurrency int
                Internal buffer (default 50)
    -d int      Update interval of the stats (in ms) (default 1000)
    -f          Don't wait for an answer before sending another
    -i          Do an iterative query instead of recursive (to stress authoritative nameservers)
    -r string   Resolver to test against (default "127.0.0.1:53")
    -random     Use random Request Identifiers for each query
    -v          Verbose logging
    -m          Number of messages to send before stopping (defaults to 1000000)
    -inf        Ignore max number of messages and send infinitely

For IPv6 resolvers, use brackets and quotes:

    dnsstress -r "[2001:4860:4860::8888]:53" -v google.com.
