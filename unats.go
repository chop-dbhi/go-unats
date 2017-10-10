package unats

import (
	"context"
	"errors"
	"time"

	"github.com/nats-io/go-nats"
	"github.com/nats-io/go-nats-streaming"
)

var (
	DefaultRequestTimeout = time.Second

	ErrInvalidConn          = errors.New("*nats.Conn or stan.Conn required")
	ErrNoStreaming          = errors.New("streaming not available")
	ErrInvalidConnectOption = errors.New("nats.Option or stan.Option required")
)

// natsMsg converts a NATS message to the Msg type.
func natsMsg(msg *nats.Msg, sub *Subscription) *Msg {
	return &Msg{
		Subject: msg.Subject,
		Reply:   msg.Reply,
		Data:    msg.Data,
		Sub:     sub,
	}
}

// stanMsg converts a STAN message to the Msg type.
func stanMsg(msg *stan.Msg, sub *Subscription) *Msg {
	return &Msg{
		Subject:     msg.Subject,
		Sequence:    msg.Sequence,
		Time:        time.Unix(0, msg.Timestamp),
		Reply:       msg.Reply,
		Data:        msg.Data,
		Redelivered: msg.Redelivered,
		CRC32:       msg.CRC32,
		Sub:         sub,
	}
}

// natsHandler returns a NATS msg handler that wraps a Handler.
func natsHandler(sub *Subscription, h Handler) nats.MsgHandler {
	return func(msg *nats.Msg) {
		h(natsMsg(msg, sub))
	}
}

// stanHandler returns a STAN msg handler that wraps a Handler.
func stanHandler(sub *Subscription, h Handler) stan.MsgHandler {
	return func(msg *stan.Msg) {
		h(stanMsg(msg, sub))
	}
}

// Handler is a message handler used for subscriptions.
type Handler func(*Msg)

// Msg is a unified type for NATS and STAN messages.
type Msg struct {
	Sequence    uint64
	Time        time.Time
	Subject     string
	Reply       string
	Data        []byte
	CRC32       uint32
	Redelivered bool
	Sub         *Subscription

	msg *stan.Msg
}

// Streaming returns true if this is a message from a STAN channel.
func (m *Msg) Streaming() bool {
	return m.msg != nil
}

// Ack is used to acknowledge receipt of a message to the server. This only
// applies to messages from STAN.
func (m *Msg) Ack() error {
	if m.msg != nil {
		return m.msg.Ack()
	}

	return nil
}

type requestOptions struct {
	Timeout time.Duration
	Context context.Context
}

func (x *requestOptions) Apply(opts ...RequestOption) {
	for _, o := range opts {
		o(x)
	}
}

// RequestOption is an option used when calling conn.Request.
type RequestOption func(*requestOptions)

// Timeout is a request option for setting an explicit request timeout.
func Timeout(d time.Duration) RequestOption {
	return func(o *requestOptions) {
		o.Timeout = d
	}
}

// Context is a request option for passing a context used for cancellation.
func Context(ctx context.Context) RequestOption {
	return func(o *requestOptions) {
		o.Context = ctx
	}
}

type subscriptionOptions struct {
	Stream bool
	Queue  string
	Reset  bool
	Stan   []stan.SubscriptionOption
}

func (x *subscriptionOptions) Apply(opts ...SubscriptionOption) {
	for _, o := range opts {
		o(x)
	}
}

// SubscriptionOption is used when creating a subscription.
type SubscriptionOption func(*subscriptionOptions)

// Stream is an option for denoting the subscription is to a STAN stream.
func Stream() SubscriptionOption {
	return func(o *subscriptionOptions) {
		o.Stream = true
	}
}

// Queue is an option for establishing a queue (multi-consumer) subscription.
// Multiple subscriptions can be made to the same queue and the server will
// distribute messages across clients.
func Queue(name string) SubscriptionOption {
	return func(o *subscriptionOptions) {
		o.Queue = name
	}
}

// Durable is an option to establishing a durable subscription to a stream.
// This option only applies to stream subscriptions.
func Durable(name string) SubscriptionOption {
	return func(o *subscriptionOptions) {
		o.Stan = append(o.Stan, stan.DurableName(name))
	}
}

// Reset resets a durable stream. This option only applies to stream subscriptions.
func Reset() SubscriptionOption {
	return func(o *subscriptionOptions) {
		o.Reset = true
	}
}

// AckWait is the duration of time for the server to wait until a delivered
// message is acknowledged. This option only applies to stream subscriptions.
func AckWait(d time.Duration) SubscriptionOption {
	return func(o *subscriptionOptions) {
		o.Stan = append(o.Stan, stan.AckWait(d))
	}
}

// ManualAck enables manual ack-ing of messages. This option only applies
// to streaming messages. This option only applies to stream subscriptions.
func ManualAck() SubscriptionOption {
	return func(o *subscriptionOptions) {
		o.Stan = append(o.Stan, stan.SetManualAckMode())
	}
}

// Buffer specifies the number of in-flight messages allowed. All in-flight
// messages will be delivered regardless if a prior message failed to process.
// This option only applies to stream subscriptions.
func Buffer(p int) SubscriptionOption {
	return func(o *subscriptionOptions) {
		o.Stan = append(o.Stan, stan.MaxInflight(p))
	}
}

// Replay has the server deliver all messages available on a stream.
// This option only applies to stream subscriptions.
func Replay() SubscriptionOption {
	return func(o *subscriptionOptions) {
		o.Stan = append(o.Stan, stan.DeliverAllAvailable())
	}
}

// Since replays messages since some offset of time. For example, Since(time.Hour)
// will replay messages received by the server within the last hour.
// This option only applies to stream subscriptions.
func Since(d time.Duration) SubscriptionOption {
	return func(o *subscriptionOptions) {
		o.Stan = append(o.Stan, stan.StartAtTimeDelta(d))
	}
}

// Asof replays messages as of some absolute point in time.
// This option only applies to stream subscriptions.
func Asof(t time.Time) SubscriptionOption {
	return func(o *subscriptionOptions) {
		o.Stan = append(o.Stan, stan.StartAtTime(t))
	}
}

// Offset replays messages starting at the specified sequence offset.
// This option only applies to stream subscriptions.
func Offset(s uint64) SubscriptionOption {
	return func(o *subscriptionOptions) {
		o.Stan = append(o.Stan, stan.StartAtSequence(s))
	}
}

// Subscription is a subscription to the cluster.
type Subscription struct {
	nc *nats.Subscription
	sc stan.Subscription
}

// Nats returns the underlying NATS subscription if applicable.
func (s *Subscription) Nats() *nats.Subscription {
	return s.nc
}

// Stan returns the underlying STAN subscription if applicable.
func (s *Subscription) Stan() stan.Subscription {
	return s.sc
}

// Streaming returns true if this is a stream-based subscription.
func (s *Subscription) Streaming() bool {
	return s.sc != nil
}

// Close closes the subscription.
func (s *Subscription) Close() error {
	if s.sc != nil {
		return s.sc.Close()
	}
	return s.nc.Unsubscribe()
}

// Unsubscribe closes the subscription and removes interest if the
// stream is durable.
func (s *Subscription) Unsubscribe() error {
	if s.sc != nil {
		return s.sc.Unsubscribe()
	}
	return s.nc.Unsubscribe()
}

type Conn struct {
	sc stan.Conn
	nc *nats.Conn
}

func (c *Conn) Close() error {
	if c.sc != nil {
		return c.sc.Close()
	}
	c.nc.Close()
	return nil
}

func (c *Conn) Streaming() bool {
	return c.sc != nil
}

func (c *Conn) PublishStream(subject string, data []byte, stream bool) error {
	if stream {
		if c.sc == nil {
			return ErrNoStreaming
		}

		return c.sc.Publish(subject, data)
	}

	return c.nc.Publish(subject, data)
}

func (c *Conn) Request(subject string, data []byte, opts ...RequestOption) (*Msg, error) {
	var o requestOptions
	o.Apply(opts...)

	var (
		err error
		msg *nats.Msg
	)

	if o.Context != nil {
		msg, err = c.nc.RequestWithContext(o.Context, subject, data)
	} else {
		t := o.Timeout
		if t == 0 {
			t = DefaultRequestTimeout
		}

		msg, err = c.nc.Request(subject, data, o.Timeout)
	}

	if err != nil {
		return nil, err
	}

	return natsMsg(msg, nil), nil
}

func (c *Conn) Subscribe(subject string, handler Handler, opts ...SubscriptionOption) (*Subscription, error) {
	var o subscriptionOptions
	o.Apply(opts...)

	var (
		err error
		sub Subscription
	)

	if o.Stream {
		if c.sc == nil {
			return nil, ErrNoStreaming
		}

		// Reset a durable subscription.
		if o.Reset {
			var tsub stan.Subscription
			if o.Queue != "" {
				tsub, err = c.sc.QueueSubscribe(subject, o.Queue, func(*stan.Msg) {}, o.Stan...)
			} else {
				tsub, err = c.sc.Subscribe(subject, func(*stan.Msg) {}, o.Stan...)
			}
			if err != nil {
				return nil, err
			}
			if err = tsub.Unsubscribe(); err != nil {
				return nil, err
			}
		}

		sh := stanHandler(&sub, handler)
		if o.Queue != "" {
			sub.sc, err = c.sc.QueueSubscribe(subject, o.Queue, sh, o.Stan...)
		} else {
			sub.sc, err = c.sc.Subscribe(subject, sh, o.Stan...)
		}
	} else {
		nh := natsHandler(&sub, handler)
		if o.Queue != "" {
			sub.nc, err = c.nc.QueueSubscribe(subject, o.Queue, nh)
		} else {
			sub.nc, err = c.nc.Subscribe(subject, nh)
		}
	}

	if err != nil {
		return nil, err
	}

	return &sub, nil
}

// New initializes a connection from an existing NATS or STAN connection.
func New(conn interface{}) (*Conn, error) {
	var c Conn

	switch x := conn.(type) {
	case *nats.Conn:
		c.nc = x
	case stan.Conn:
		c.sc = x
		c.nc = x.NatsConn()
	default:
		return nil, ErrInvalidConn
	}

	return &c, nil
}

// Connect establishes a connection to a NATS cluster with streaming
// support specified and available.
func Connect(url, cluster, client string, opts ...interface{}) (*Conn, error) {
	var (
		natsOpts []nats.Option
		stanOpts []stan.Option
	)

	for _, o := range opts {
		switch x := o.(type) {
		case nats.Option:
			natsOpts = append(natsOpts, x)
		case stan.Option:
			stanOpts = append(stanOpts, x)
		default:
			return nil, ErrInvalidConnectOption
		}
	}

	// Create NATS connection first.
	nc, err := nats.Connect(url, natsOpts...)
	if err != nil {
		return nil, err
	}

	var sc stan.Conn

	// Streaming requested.
	if cluster != "" {
		// Use NATs connection.
		stanOpts = append(stanOpts, stan.NatsConn(nc))

		sc, err = stan.Connect(cluster, client, stanOpts...)
		if err != nil {
			return nil, err
		}
	}

	c := Conn{
		nc: nc,
		sc: sc,
	}

	return &c, nil
}
