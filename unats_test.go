package unats

import (
	"flag"
	"testing"
	"time"

	"github.com/nats-io/go-nats"
	"github.com/nats-io/go-nats-streaming"
)

var (
	natsUrl     string
	stanClient  string
	stanCluster string
)

func init() {
	flag.StringVar(&natsUrl, "url", "nats://localhost:4222", "NATS URL")
	flag.StringVar(&stanClient, "client", "test-client", "STAN client")
	flag.StringVar(&stanCluster, "cluster", "test-cluster", "STAN cluster")

	flag.Parse()
}

func withNats(t *testing.T) *Conn {
	nc, err := nats.Connect(natsUrl)
	if err != nil {
		t.Fatal(err)
	}

	c, _ := New(nc)
	return c
}

func withStan(t *testing.T) *Conn {
	sc, err := stan.Connect(stanCluster, stanClient, stan.NatsURL(natsUrl))
	if err != nil {
		t.Fatal(err)
	}

	c, _ := New(sc)
	return c
}

func TestNatsSubscribe(t *testing.T) {
	c := withNats(t)
	defer c.Close()

	// NATS subscription.
	sub, err := c.Subscribe(
		"test-nats",
		func(msg *Msg) {},
	)
	if err != nil {
		t.Fatal(err)
	}
	if sub.Streaming() {
		t.Error("did not expect STAN subscription")
	}
	sub.Close()

	// Queue subscribe.
	sub, err = c.Subscribe(
		"test-nats",
		func(msg *Msg) {},
		Queue("test-nats-queue"),
	)
	if err != nil {
		t.Fatal(err)
	}
	if sub.Streaming() {
		t.Error("did not expect STAN subscription")
	}
	sub.Close()
}

func TestNatsSubscribeError(t *testing.T) {
	c := withNats(t)
	defer c.Close()

	// Try unsupported stream.
	_, err := c.Subscribe(
		"test-nats",
		func(msg *Msg) {},
		Stream(),
	)
	if err != ErrNoStreaming {
		t.Fatal("expected no streaming error")
	}
}

func TestStanSubscribe(t *testing.T) {
	c := withStan(t)
	defer c.Close()

	// Explicit using Stream.
	sub, err := c.Subscribe(
		"test-stan",
		func(msg *Msg) {},
		Stream(),
	)
	if err != nil {
		t.Fatal(err)
	}
	if !sub.Streaming() {
		t.Error("expected STAN subscription")
	}
	sub.Close()

	// Implicit using streaming features.
	sub, err = c.Subscribe(
		"test-stan",
		func(msg *Msg) {},
		Replay(),
		Buffer(1),
		AckWait(time.Second),
		Stream(),
	)
	if err != nil {
		t.Fatal(err)
	}
	if !sub.Streaming() {
		t.Error("expected STAN subscription")
	}
	sub.Close()
}
