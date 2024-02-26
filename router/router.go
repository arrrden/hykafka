package router

import (
	"context"
	"encoding/json"
	"fmt"
	"io"

	kafka "github.com/arrrden/hykafka"
	gkafka "github.com/segmentio/kafka-go"
)

type (
	MsgType = string
	CtxKey  string
)

const (
	CtxKeyMsgID   CtxKey = "k-msgid"
	CtxKeyMsgName CtxKey = "k-msgname"
)

const (
	MsgTypeEvent    MsgType = "EVENT"
	MsgTypeErrEvent MsgType = "ERR_EVENT"
	MsgTypeUnk      MsgType = "UNK"
)

type Router interface {
	io.Closer
	Listen() error
	NewRouteGroup(topic string, defHandler MsgHandler) RouteGroup
	RouteGroup(topic string) (RouteGroup, error)
	RqTopics() []string
}

type RouteGroup interface {
	SetMsgNameResolver(r ResolveMsgName)
	HandleMsg(msgName string, handler MsgHandler)
	MsgHandler(msgName string) (MsgHandler, error)
	ResolveMsgName(msg interface{}) (string, error)
}

type MsgHandler func(ctx context.Context, data []byte) error

type ResolveMsgName func(msg interface{}) (string, error)

type TopicRouteGroup struct {
	topic       string
	defHandler  MsgHandler
	msgResolver ResolveMsgName
	handlers    map[string]MsgHandler
}

type NewRouterOptions struct {
	GroupId  string
	ErrTopic string
}

func NewRouter(ctx context.Context, kafkaClient *kafka.KafkaClient, opts *NewRouterOptions) Router {
	r := &RouterImpl{
		ctx:       ctx,
		client:    kafkaClient,
		routeGrps: make(map[string]*TopicRouteGroup),
	}

	if opts != nil {
		r.errTopic = opts.ErrTopic
		r.groupId = opts.GroupId
	}

	return r
}

type RouterImpl struct {
	io.Closer
	ctx       context.Context
	client    *kafka.KafkaClient
	groupId   string
	routeGrps map[string]*TopicRouteGroup
	errTopic  string
	stop      chan interface{}
}

// MsgHandler returns the handler for the route.
func (rg *TopicRouteGroup) MsgHandler(msgName string) (MsgHandler, error) {
	h, ok := rg.handlers[msgName]
	if !ok {
		if rg.defHandler == nil {
			return nil, fmt.Errorf("handler for given message %s not found", msgName)
		}
		return rg.defHandler, nil
	}
	return h, nil
}

func (rg *TopicRouteGroup) HandleMsg(msgName string, handler MsgHandler) {
	rg.handlers[msgName] = handler
}

func (rg *TopicRouteGroup) SetMsgNameResolver(r ResolveMsgName) {
	rg.msgResolver = r
}

func (rg *TopicRouteGroup) ResolveMsgName(msg interface{}) (string, error) {
	return rg.msgResolver(msg)
}

func (r *RouterImpl) NewRouteGroup(topic string, defHandler MsgHandler) RouteGroup {
	rg := &TopicRouteGroup{
		topic:       topic,
		msgResolver: resolveMsgName,
		defHandler:  defHandler,
		handlers:    make(map[string]MsgHandler),
	}
	r.routeGrps[topic] = rg
	return rg
}

func (r *RouterImpl) RqTopics() []string {
	var topics []string
	for t := range r.routeGrps {
		topics = append(topics, t)
	}
	return topics
}

func (r *RouterImpl) Listen() error {
	conn, err := r.client.NewConnection()
	if err != nil {
		return err
	}

	defer conn.Close()
	msgCh := make(chan kafka.Message)
	errCh := make(chan error)

	subscribed := true

	go func() {
		conn.Subscribe(r.RqTopics(), r.groupId, msgCh, errCh, &subscribed)
	}()

	go func() {
	loop:
		for {
			if !subscribed {
				break loop
			}

			select {
			case msg := <-msgCh:
				r.callHandler(&msg)
			case errs := <-errCh:
				if errs != nil && r.errTopic != "" {
					err = conn.Produce(r.errTopic, kafka.Message{
						Partition: int(gkafka.PatternTypeAny),
						Key:       []byte("error"),
						Value:     []byte(errs.Error()),
						Headers: []gkafka.Header{
							{Key: kafka.MsgHeaderMessageType, Value: []byte(MsgTypeErrEvent)},
						},
					})
					if err != nil {
						fmt.Println("failed to produce error on error topic: ", err.Error())
					}
				} else {
					fmt.Println("received error from listener: ", err.Error())
				}
			}
		}
	}()

	<-r.stop
	close(r.stop)
	subscribed = false

	return nil
}

func (r *RouterImpl) Close() error {
	r.stop <- struct{}{}
	return nil
}

func (r *RouterImpl) callHandler(msg *kafka.Message) error {
	r.ctx = context.WithValue(r.ctx, CtxKeyMsgID, string(msg.Key))

	topic := msg.Topic

	rg, err := r.RouteGroup(topic)
	if err != nil {
		r.writeErr(MsgTypeUnk, msg.Key, r.errTopic, err)
		return err
	}

	msgName, err := rg.ResolveMsgName(msg)
	if err != nil {
		r.writeErr(MsgTypeUnk, msg.Key, r.errTopic, err)
		return err
	}

	if msgName != kafka.MsgHeaderValueUNK {
		r.ctx = context.WithValue(r.ctx, CtxKeyMsgName, msgName)
	}

	h, err := rg.MsgHandler(msgName)
	if err != nil {
		r.writeErr(msgName, msg.Key, r.errTopic, err)
		return err
	}

	if err := h(r.ctx, msg.Value); err != nil {
		r.writeErr(msgName, msg.Key, r.errTopic, err)
		return err
	}
	return nil
}

func (r *RouterImpl) RouteGroup(topic string) (RouteGroup, error) {
	rg, ok := r.routeGrps[topic]
	if !ok {
		return nil, fmt.Errorf("routing group for given topic name '%s' not found", topic)
	}
	return rg, nil
}

func (r *RouterImpl) MsgHandler(topic, msgName string) (MsgHandler, error) {
	rg, ok := r.routeGrps[topic]
	if !ok {
		return nil, fmt.Errorf("routing group for given topic name '%s' not found", topic)
	}
	return rg.MsgHandler(msgName)
}

func (r *RouterImpl) writeErr(msgName string, msgKey []byte, errTopic string, err error) error {
	if err == nil {
		return nil
	}

	conn, err := r.client.NewConnection()
	if err != nil {
		return err
	}

	defer conn.Close()

	switch err.(type) {
	case nil:
		return nil
	case gkafka.WriteErrors:
		errb, err := json.Marshal(err)
		if err != nil {
			return fmt.Errorf("failed to marshal error: %w", err)
		}
		err = conn.Produce(errTopic, kafka.Message{
			Partition: int(gkafka.PatternTypeAny),
			Topic:     errTopic,
			Key:       msgKey,
			Value:     errb,
			Headers: []gkafka.Header{
				{Key: kafka.MsgHeaderMessageName, Value: []byte(msgName)},
				{Key: kafka.MsgHeaderMessageType, Value: []byte(MsgTypeErrEvent)},
			},
		})
		if err != nil {
			return fmt.Errorf("failed to produce error on error topic: %w", err)
		}
	default:
		return fmt.Errorf("unhandled error: %w", err)
	}

	return nil
}

func headerByKey(hdrs []gkafka.Header, key string) MsgType {
	for _, h := range hdrs {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return kafka.MsgHeaderValueUNK
}

func resolveMsgName(msg interface{}) (string, error) {
	return headerByKey(msg.(*kafka.Message).Headers, kafka.MsgHeaderMessageName), nil
}
