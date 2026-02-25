package internal

import (
	"encoding/binary"
	"fmt"
	"io"
	"time"
)

const (
	MsgTypeWALFrame  uint8 = 1
	MsgTypeHeartbeat uint8 = 2
	MsgTypeAck       uint8 = 3

	// Header: type(1) + sequence(8) + timestamp(8) + payload_len(4) = 21 bytes
	HeaderSize = 21

	ProtocolVersion uint8 = 1
)

type Message struct {
	Type       uint8
	Sequence   uint64
	Timestamp  int64
	PayloadLen uint32
	Payload    []byte
}

func NewWALFrame(seq uint64, data []byte) *Message {
	return &Message{
		Type:       MsgTypeWALFrame,
		Sequence:   seq,
		Timestamp:  time.Now().UnixMilli(),
		PayloadLen: uint32(len(data)),
		Payload:    data,
	}
}

func NewHeartbeat(seq uint64) *Message {
	return &Message{
		Type:      MsgTypeHeartbeat,
		Sequence:  seq,
		Timestamp: time.Now().UnixMilli(),
	}
}

func NewAck(seq uint64) *Message {
	return &Message{
		Type:      MsgTypeAck,
		Sequence:  seq,
		Timestamp: time.Now().UnixMilli(),
	}
}

// Encode writes the message in binary format to the writer.
func (m *Message) Encode(w io.Writer) error {
	header := make([]byte, HeaderSize)
	header[0] = m.Type
	binary.BigEndian.PutUint64(header[1:9], m.Sequence)
	binary.BigEndian.PutUint64(header[9:17], uint64(m.Timestamp))
	binary.BigEndian.PutUint32(header[17:21], m.PayloadLen)

	if _, err := w.Write(header); err != nil {
		return fmt.Errorf("write header: %w", err)
	}

	if m.PayloadLen > 0 && len(m.Payload) > 0 {
		if _, err := w.Write(m.Payload); err != nil {
			return fmt.Errorf("write payload: %w", err)
		}
	}
	return nil
}

// Decode reads a message from the reader. Max payload: 64 MB.
func Decode(r io.Reader) (*Message, error) {
	header := make([]byte, HeaderSize)
	if _, err := io.ReadFull(r, header); err != nil {
		return nil, fmt.Errorf("read header: %w", err)
	}

	msg := &Message{
		Type:       header[0],
		Sequence:   binary.BigEndian.Uint64(header[1:9]),
		Timestamp:  int64(binary.BigEndian.Uint64(header[9:17])),
		PayloadLen: binary.BigEndian.Uint32(header[17:21]),
	}

	if msg.PayloadLen > 64*1024*1024 {
		return nil, fmt.Errorf("payload too large: %d bytes", msg.PayloadLen)
	}

	if msg.PayloadLen > 0 {
		msg.Payload = make([]byte, msg.PayloadLen)
		if _, err := io.ReadFull(r, msg.Payload); err != nil {
			return nil, fmt.Errorf("read payload: %w", err)
		}
	}

	return msg, nil
}
