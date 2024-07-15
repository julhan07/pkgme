package rabbitmq

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/streadway/amqp"
)

const (
	reconnectDelay = 5 * time.Second
	maxRetries     = 5
)

type RabbitMQConfig struct {
	Host     string
	Port     int
	User     string
	Password string
}

type ChannelConfig struct {
	Exchange string
	Key      string
	Queue    string
	Tag      string
	Type     string
}

type RabbitMQ struct {
	conn        *amqp.Connection
	channel     *amqp.Channel
	Config      RabbitMQConfig
	ChannelConf ChannelConfig
}

func NewRabbitMQ(conf *RabbitMQConfig, channelConf *ChannelConfig) *RabbitMQ {
	if conf == nil || channelConf == nil {
		log.Fatalf("Configuration cannot be nil")
	}
	rbbmq := &RabbitMQ{
		Config:      *conf,
		ChannelConf: *channelConf,
	}
	return rbbmq
}

func (r *RabbitMQ) InitConnection() error {
	err := r.connect()
	if err != nil {
		return err
	}

	err = r.ExchangeDeclare()
	if err != nil {
		return err
	}

	err = r.QueueDeclare()
	if err != nil {
		return err
	}

	err = r.QueueBind()
	if err != nil {
		return err
	}

	return nil
}

func (r *RabbitMQ) connect() error {
	var err error
	r.conn, err = r.Conn()
	if err != nil {
		return err
	}

	r.channel, err = r.Channel(r.conn)
	if err != nil {
		return err
	}

	// Handle connection closure
	r.channel.NotifyClose(make(chan *amqp.Error))
	return nil
}

func (r *RabbitMQ) reconnect() error {
	for attempts := 1; attempts <= maxRetries; attempts++ {
		err := r.connect()
		if err == nil {
			return nil
		}
		log.Printf("Failed to reconnect to RabbitMQ (attempt %d/%d): %v. Retrying in %v...", attempts, maxRetries, err, reconnectDelay)
		time.Sleep(reconnectDelay)
	}
	return fmt.Errorf("failed to reconnect to RabbitMQ after %d attempts", maxRetries)
}

func (r *RabbitMQ) Publish(message interface{}) error {
	if r.channel == nil {
		return fmt.Errorf("channel is not initialized")
	}

	jsonMsg, err := json.Marshal(message)
	if err != nil {
		return err
	}

	err = r.channel.Publish(
		r.ChannelConf.Exchange, // exchange
		r.ChannelConf.Key,      // routing key
		false,                  // mandatory
		false,                  // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        jsonMsg,
		})
	if err != nil {
		log.Printf("Failed to publish message: %v. Reconnecting and retrying...", err)
		reconnectErr := r.reconnect()
		if reconnectErr != nil {
			return reconnectErr
		}
		return r.Publish(message)
	}

	log.Printf("Published message: %s", message)
	return nil
}

func (r *RabbitMQ) Consume() (<-chan amqp.Delivery, error) {
	if r.channel == nil {
		return nil, fmt.Errorf("channel is not initialized")
	}

	msgs, err := r.channel.Consume(
		r.ChannelConf.Queue, // queue
		r.ChannelConf.Tag,   // consumer
		false,               // auto-ack
		false,               // exclusive
		false,               // no-local
		false,               // no-wait
		nil,                 // args
	)
	if err != nil {
		log.Printf("Failed to consume messages: %v. Reconnecting...", err)
		reconnectErr := r.reconnect()
		if reconnectErr != nil {
			return nil, reconnectErr
		}
		return r.Consume()
	}

	return msgs, nil
}

func (r *RabbitMQ) Conn() (*amqp.Connection, error) {
	url := fmt.Sprintf("amqp://%v:%v@%v:%v/", r.Config.User, r.Config.Password, r.Config.Host, r.Config.Port)
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %v", err)
	}

	return conn, nil
}

func (r *RabbitMQ) Channel(conn *amqp.Connection) (*amqp.Channel, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("failed to open a channel: %v", err)
	}

	return ch, nil
}

func (r *RabbitMQ) ExchangeDeclare() error {
	err := r.channel.ExchangeDeclare(
		r.ChannelConf.Exchange, // name
		r.ChannelConf.Type,     // type
		true,                   // durable
		false,                  // auto-deleted
		false,                  // internal
		false,                  // no-wait
		nil,                    // arguments
	)
	if err != nil {
		return fmt.Errorf("failed to declare exchange: %v", err)
	}
	return nil
}

func (r *RabbitMQ) QueueDeclare() error {
	_, err := r.channel.QueueDeclare(
		r.ChannelConf.Queue, // name
		true,                // durable
		false,               // delete when unused
		false,               // exclusive
		false,               // no-wait
		nil,                 // arguments
	)
	if err != nil {
		return fmt.Errorf("failed to declare queue: %v", err)
	}
	return nil
}

func (r *RabbitMQ) QueueBind() error {
	err := r.channel.QueueBind(
		r.ChannelConf.Queue,    // queue name
		r.ChannelConf.Key,      // routing key
		r.ChannelConf.Exchange, // exchange
		false,
		nil,
	)
	if err != nil {
		return fmt.Errorf("failed to bind queue: %v", err)
	}
	return nil
}

func (r *RabbitMQ) Close() {
	if r.channel != nil {
		r.channel.Close()
	}
	if r.conn != nil {
		r.conn.Close()
	}
}
