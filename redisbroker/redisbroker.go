package redisbroker

import (
	"context"
	"sync"

	"github.com/go-redis/redis/v8"
	"go.uber.org/zap"
)

type RedisBroker struct {
	redisClient       *redis.Client
	deviceSubscribers map[string][]chan []byte
	mu                *sync.Mutex
	pubsub            *redis.PubSub
	logger            *zap.Logger
}

func New(ctx context.Context, redisClient *redis.Client, logger *zap.Logger) *RedisBroker {
	return &RedisBroker{
		redisClient:       redisClient,
		deviceSubscribers: make(map[string][]chan []byte),
		mu:                &sync.Mutex{},
		pubsub:            redisClient.Subscribe(ctx, "lsdk"), // empty subscription
		logger:            logger,
	}
}

func (r *RedisBroker) Subscribe(ctx context.Context, channelID string) chan []byte {
	deviceChannel := make(chan []byte)
	r.mu.Lock()
	r.deviceSubscribers[channelID] = append(r.deviceSubscribers[channelID], deviceChannel)
	r.mu.Unlock()
	r.redisClient.Subscribe(ctx, channelID)
	r.logger.Debug("subscribe pubsub", zap.String("channelID", channelID), zap.Int("number of subscribers for deviceID", len(r.deviceSubscribers[channelID])), zap.Int("number of devices subscribed", len(r.deviceSubscribers)))
	return deviceChannel
}

func (r *RedisBroker) Unsubscribe(ctx context.Context, deviceChannel chan []byte) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for channelID, subscribers := range r.deviceSubscribers {
		for i, haystack := range subscribers {
			if haystack == deviceChannel {
				r.deviceSubscribers[channelID] = remove(subscribers, i)
				if len(r.deviceSubscribers[channelID]) == 0 {
					err := r.pubsub.Unsubscribe(ctx, channelID)
					if err != nil {
						r.logger.Error("could not unsubscribe pubsub", zap.String("channelID", channelID), zap.Error(err))
					}
				}
			}
		}
	}
}

func (r *RedisBroker) Listen() {
	for data := range r.pubsub.Channel() {
		channelID := data.Channel
		r.mu.Lock()
		subscribers, found := r.deviceSubscribers[channelID]
		r.mu.Unlock()
		if !found {
			r.logger.Warn("did not find subscribers for message channel", zap.String("channelID", channelID), zap.Any("pubsub message", data))
			continue
		}
		for _, subscriber := range subscribers {
			subscriber <- []byte(data.Payload)
		}
	}
}

func (r *RedisBroker) Shutdown(ctx context.Context) {
	if r.pubsub != nil {
		_ = r.pubsub.Unsubscribe(ctx)
		_ = r.pubsub.Close()
		r.pubsub = nil
	}
}

func remove[T any](slice []T, s int) []T {
	return append(slice[:s], slice[s+1:]...)
}

// Usage:
/*
 listener := listen.NewListener()
 listener.Subscribe("M1")//subscribe for a meter id
 channel,_ := listener.Listen()
 for message := range channel {
// Process message
 }
// Add additional meter:
 listener.Subscribe("M2")
// Unsubscribe:
 listener.Unsubscribe("M2")
// Client disconnects
 listener.Shutdown()


var redisAddress string
var channelPrefix string

func NewClient() RedisClient {
	redisAddress = os.Getenv("REDIS_ADDRESS")
	if redisAddress == "" {
		return nil
	}
	channelPrefix = os.Getenv("REDIS_CHANNEL_PREFIX")

	log.Infof("Redis address: %v, channel prefix: %v", redisAddress, channelPrefix)
	listenClient := redis.NewClient(&redis.Options{
		Addr: redisAddress,
	})

	/*
		// Check we can reach redis
		_, err := listenClient.Ping().Result()
		if err != nil {
			log.Panic(fmt.Sprintf("Ping of redis failed: %v", err))
		}

	return &redisClient{listenClient: listenClient}
}

type RedisClient interface {
	Shutdown()
	NewListener() *RedisListener
	Ping() error
}

type redisClient struct {
	listenClient *redis.Client
}

type RedisListener struct {
	pubsub      *redis.PubSub
	redisClient *redisClient
}

func (c *redisClient) NewListener() *RedisListener {
	if redisAddress == "" {
		return nil
	}
	return &RedisListener{redisClient: c}
}

func (c *redisClient) Shutdown() {
	if c.listenClient != nil {
		c.listenClient.Shutdown()
	}
	c.listenClient = nil
}

func subscriptionID(meterID string) string {
	if meterID == "" {
		return ""
	}
	return channelPrefix + meterID
}

func (c *redisClient) Ping() error {
	_, err := c.listenClient.Ping().Result()
	return err
}

func (r *RedisListener) Subscribe(meterID string) {
	subscriptionID := subscriptionID(meterID)
	if r.pubsub == nil {
		r.pubsub = r.redisClient.listenClient.Subscribe(subscriptionID)
	} else {
		// Add to existing subscription
		_ = r.pubsub.Subscribe(subscriptionID)
	}
}

func (r *RedisListener) Listen(incomingReadings chan *hanbridge.HanReading) error {
	if r.pubsub == nil {
		return errors.New("no subscription created")
	}

	go func() {
		channel := r.pubsub.Channel()
		for data := range channel {
			data.Channel
			var redisMessage hanbridge.RedisReading
			err := proto.Unmarshal([]byte(data.Payload), &redisMessage)
			if err == nil {
				var reading hanbridge.HanReading
				err = proto.Unmarshal(redisMessage.Data, &reading)
				if err == nil {
					incomingReadings <- &reading
				}
			}
		}
	}()

	return nil
}

func (r *RedisListener) Unsubscribe(meterID string) {
	if r.pubsub != nil {
		subscriptionID := subscriptionID(meterID)
		_ = r.pubsub.Unsubscribe(subscriptionID)
	}
}

func (r *RedisListener) Shutdown() {
	if r.pubsub != nil {
		_ = r.pubsub.Unsubscribe()
		_ = r.pubsub.Close()
		r.pubsub = nil
	}
}
*/
