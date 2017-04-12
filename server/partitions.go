// Copyright 2017 Apcera Inc. All rights reserved.

package server

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"time"

	natsd "github.com/nats-io/gnatsd/server"
	"github.com/nats-io/go-nats"
	"github.com/nats-io/nats-streaming-server/spb"
	"github.com/nats-io/nats-streaming-server/stores"
	"github.com/nats-io/nats-streaming-server/util"
)

// Constants related to partitioning
const (
	// Prefix of subject to send list of channels in this server's partition
	partitionsPrefix = "_STAN.part"
	// Default interval we check for topology change
	partitionsDefaultCheckInterval = time.Second
	// Default timeout for a server to wait for replies to its request
	partitionsDefaultNextMsgTimeout = time.Second
	// This is the value that is stored in the sublist for a given subject
	channelInterest = 1
)

// So that we can override in tests
var (
	partitionsCheckInterval  = partitionsDefaultCheckInterval
	partitionsNextMsgTimeout = partitionsDefaultNextMsgTimeout
	partitionsNoPanic        = false
)

type partitions struct {
	s               *StanServer
	channels        []string
	sl              *util.Sublist
	nc              *nats.Conn
	sendListSubject string
	ctrlMsgSubject  string // send to this subject when processing close/unsub requests
	servers         map[string]struct{}
	msgsCh          chan *nats.Msg
	numGoRoutines   int // total of go routines used by partitioning code
	quitCh          chan struct{}
}

// Creates the subscription for partitioning communication, sends
// the initial request (in case there are other servers listening) and
// starts the go routines handling HBs and cleanup of servers map.
func (s *StanServer) initPartitions(sOpts *Options, nOpts *natsd.Options, storeChannels map[string]*stores.ChannelLimits) error {
	// The option says that the server should only use the pre-defined channels,
	// but none was specified. Don't see the point in continuing...
	if len(storeChannels) == 0 {
		return ErrNoChannel
	}
	nc, err := s.createNatsClientConn("pc", sOpts, nOpts)
	if err != nil {
		return err
	}
	p := &partitions{
		s:  s,
		nc: nc,
	}
	// Now that the connection is created, we need to set s.partitioning to cp
	// so that server shutdown can properly close this connection.
	s.partitions = p
	p.createChannelsMapAndSublist(storeChannels)
	p.sendListSubject = partitionsPrefix + "." + sOpts.ID
	// Use the partitions' own connection for channels list requests
	sub, err := p.nc.Subscribe(p.sendListSubject, p.processChannelsListRequests)
	if err != nil {
		return fmt.Errorf("unable to subscribe: %v", err)
	}
	sub.SetPendingLimits(-1, -1)
	p.servers = make(map[string]struct{})
	// Capture the known list of NATS Servers before sending our list.
	// If later the known servers change, it will be interpreted as
	// a change in the NATS cluster, so we will send our channels list
	// again.
	p.updateServersMap(p.nc.DiscoveredServers())
	// Now send our list and check if any server is complaining
	// about having one channel in common.
	if err := p.checkChannelsUniqueInCluster(); err != nil {
		return err
	}
	p.msgsCh = make(chan *nats.Msg, 65536)
	p.numGoRoutines = 2
	p.quitCh = make(chan struct{}, p.numGoRoutines)
	s.wg.Add(p.numGoRoutines)
	go p.checkForTopologyChanges()
	go p.postClientPublishIncomingMsgs()
	return nil
}

// Creates the channels map based on the store's PerChannel map that was given.
func (p *partitions) createChannelsMapAndSublist(storeChannels map[string]*stores.ChannelLimits) {
	p.channels = make([]string, 0, len(storeChannels))
	p.sl = util.NewSublist()
	for c := range storeChannels {
		p.channels = append(p.channels, c)
		// When creating the store, we have already checked that channel names
		// were valid. So this call cannot fail.
		p.sl.Insert(c, channelInterest)
	}
}

// Adds any element from the given array into scServers and return
// true if any were added, false otherwise.
func (p *partitions) updateServersMap(servers []string) bool {
	hasNew := false
	for _, url := range servers {
		if _, present := p.servers[url]; !present {
			hasNew = true
			p.servers[url] = struct{}{}
		}
	}
	return hasNew
}

// Checks at regular intervals if the NATS topology has changed
// (based on the list of discovered servers). If so, sends the
// list of channels.
// Would be nice to be able to provide a callback to NATS that
// would be invoked whenever the client library processes an
// async INFO and that the array changed.
func (p *partitions) checkForTopologyChanges() {
	defer p.s.wg.Done()
	for {
		select {
		case <-p.quitCh:
			return
		case <-time.After(partitionsCheckInterval):
			servers := p.nc.DiscoveredServers()
			if hasNew := p.updateServersMap(servers); hasNew {
				if err := p.checkChannelsUniqueInCluster(); err != nil {
					// If server is started from command line, the Fatalf
					// call will cause the process to exit. If the server
					// is run programmatically and no logger has been set
					// we need to exit with the panic.
					Fatalf("Partitioning error: %v", err)
					// For tests
					if partitionsNoPanic {
						p.s.setLastError(err)
						continue
					}
					panic(err)
				}
			}
		}
	}
}

// We use a channel subscription in order to minimize the number
// of go routines for all the explicit pub subscriptions.
// This go routine simply pulls a message from the channel and
// invokes processClientPublish(). We may have slow consumer issues,
// if so, will have to figure out another way.
func (p *partitions) postClientPublishIncomingMsgs() {
	defer p.s.wg.Done()
	for {
		select {
		case <-p.quitCh:
			return
		case m := <-p.msgsCh:
			p.s.processClientPublish(m)
		}
	}
}

// Create the internal subscriptions on the list of channels.
func (p *partitions) initSubscriptions() error {
	// NOTE: Use the server's nc connection here, not the partitions' one.
	for _, channelName := range p.channels {
		pubSubject := fmt.Sprintf("%s.%s", p.s.info.Publish, channelName)
		if _, err := p.s.nc.ChanSubscribe(pubSubject, p.msgsCh); err != nil {
			return fmt.Errorf("could not subscribe to publish subject %q, %v", channelName, err)
		}
	}
	// Add a dedicated subject for when we try to schedule a control
	// message to be processed after all messages from a given client
	// have been processed.
	p.ctrlMsgSubject = fmt.Sprintf("%s.%s", p.s.info.Publish, nats.NewInbox())
	if _, err := p.s.nc.ChanSubscribe(p.ctrlMsgSubject, p.msgsCh); err != nil {
		return fmt.Errorf("could not subscribe to subject %q, %v", p.ctrlMsgSubject, err)
	}
	return nil
}

// Sends a request to the rest of the cluster and wait a bit for
// responses (we don't know if or how many servers there may be).
// No server lock used since this is called inside RunServerWithOpts().
func (p *partitions) checkChannelsUniqueInCluster() error {
	// We will use a subscription on an inbox to get the replies
	replyInbox := nats.NewInbox()
	replySub, err := p.nc.SubscribeSync(replyInbox)
	if err != nil {
		return fmt.Errorf("unable to create sync subscription: %v", err)
	}
	defer replySub.Unsubscribe()
	// Send our list
	if err := p.sendChannelsList(replyInbox); err != nil {
		return fmt.Errorf("unable to send channels list: %v", err)
	}
	// Since we don't know how many servers are out there, keep
	// calling NextMsg until we get a timeout
	for {
		reply, err := replySub.NextMsg(partitionsNextMsgTimeout)
		if err == nats.ErrTimeout {
			return nil
		}
		if err != nil {
			return fmt.Errorf("unable to get partitioning reply: %v", err)
		}
		resp := spb.CtrlMsg{}
		if err := resp.Unmarshal(reply.Data); err != nil {
			return fmt.Errorf("unable to decode partitioning response: %v", err)
		}
		if len(resp.Data) > 0 {
			return fmt.Errorf("channel %q causes conflict with channels on server %q",
				string(resp.Data), resp.ServerID)
		}
	}
}

// Sends the list of channels to a known subject, possibly splitting the list
// in several requests if it cannot fit in a single message.
func (p *partitions) sendChannelsList(replyInbox string) error {
	sendReq := func(channels []string) error {
		buf := &bytes.Buffer{}
		gob.NewEncoder(buf).Encode(channels)
		req := &spb.CtrlMsg{
			ServerID: p.s.serverID,
			MsgType:  spb.CtrlMsg_Partitioning,
			Data:     buf.Bytes(),
		}
		reqBytes, _ := req.Marshal()
		return p.nc.PublishRequest(p.sendListSubject, replyInbox, reqBytes)
	}
	// Since the NATS message payload is limited, we need to repeat
	// requests if all channels can't fit in a request.
	maxPayload := p.nc.MaxPayload()
	start := 0
	end := 0
	for end < len(p.channels) {
		size := 0
		tmpBuf := &bytes.Buffer{}
		encoderTmpBuf := gob.NewEncoder(tmpBuf)
		for end = start; end < len(p.channels); end++ {
			encoderTmpBuf.Encode(p.channels[end])
			size += tmpBuf.Len()
			// Leave room for the CtrlMsg header
			if size > int(maxPayload-50) {
				break
			}
			tmpBuf.Reset()
		}
		if err := sendReq(p.channels[start:end]); err != nil {
			return err
		}
		start = end
	}
	return p.nc.Flush()
}

// Decode the incoming partitioning protocol message.
// It can be an HB, in which case, if it is from a new server
// we send our list to the cluster, or it can be a request
// from another server. If so, we reply to the given inbox
// with either an empty Data field or the name of the first
// channel we have in common.
func (p *partitions) processChannelsListRequests(m *nats.Msg) {
	// Message cannot be empty, we are supposed to receive
	// a spb.CtrlMsg_Partitioning protocol. We should also
	// have a repy subject
	if len(m.Data) == 0 || m.Reply == "" {
		return
	}
	req := spb.CtrlMsg{}
	if err := req.Unmarshal(m.Data); err != nil {
		Errorf("Error processing partitioning request: %v", err)
		return
	}
	// If this is our own request, ignore
	if req.ServerID == p.s.serverID {
		return
	}
	channels := []string{}
	buf := &bytes.Buffer{}
	buf.Write(req.Data)
	if err := gob.NewDecoder(buf).Decode(&channels); err != nil {
		Errorf("Error preparing request: %v", err)
		return
	}
	// Check that we don't have any of these channels defined.
	// If we do, send a reply with simply the name of the offending
	// channel in reply.Data
	reply := spb.CtrlMsg{
		ServerID: p.s.serverID,
		MsgType:  spb.CtrlMsg_Partitioning,
	}
	gotError := false
	sl := util.NewSublist()
	for _, c := range channels {
		if r := p.sl.Match(c); len(r) > 0 {
			reply.Data = []byte(c)
			gotError = true
			break
		}
		sl.Insert(c, channelInterest)
	}
	if !gotError {
		// Go over our channels and check with the other server sublist
		for _, c := range p.channels {
			if r := sl.Match(c); len(r) > 0 {
				reply.Data = []byte(c)
				break
			}
		}
	}
	replyBytes, _ := reply.Marshal()
	// If there is no duplicate, reply.Data will be empty, which means
	// that there was no conflict.
	if err := p.nc.Publish(m.Reply, replyBytes); err != nil {
		Errorf("Error sending reply to partitioning request: %v", err)
	}
}

// Notifies all go-routines used by partitioning code that the
// server is shuting down and closes the internal NATS connection.
func (p *partitions) shutdown() {
	if p.quitCh != nil {
		// Notify all go routines
		for i := 0; i < p.numGoRoutines; i++ {
			p.quitCh <- struct{}{}
		}
	}
	if p.nc != nil {
		p.nc.Close()
	}
}
