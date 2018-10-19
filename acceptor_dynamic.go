package quickfix

import (
	"bufio"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"runtime/debug"
	"strconv"
	"sync"

	"github.com/quickfixgo/quickfix/config"
)

// Verify new connection
type VerifyConnection func(*Message, *SessionID) MessageRejectError

//AcceptorDynamic accepts connections from FIX clients and manages the associated sessions.
type AcceptorDynamic struct {
	*Acceptor
	verifier VerifyConnection
	sessLock sync.Mutex
}

//Start accepting connections.
func (a *AcceptorDynamic) Start() error {
	socketAcceptHost := ""
	if a.settings.GlobalSettings().HasSetting(config.SocketAcceptHost) {
		var err error
		if socketAcceptHost, err = a.settings.GlobalSettings().Setting(config.SocketAcceptHost); err != nil {
			return err
		}
	}

	socketAcceptPort, err := a.settings.GlobalSettings().IntSetting(config.SocketAcceptPort)
	if err != nil {
		return err
	}

	var tlsConfig *tls.Config
	if tlsConfig, err = loadTLSConfigWithoutVerifyClientCert(a.settings.GlobalSettings()); err != nil {
		return err
	}

	address := net.JoinHostPort(socketAcceptHost, strconv.Itoa(socketAcceptPort))
	if tlsConfig != nil {
		if a.listener, err = tls.Listen("tcp", address, tlsConfig); err != nil {
			return err
		}
	} else {
		if a.listener, err = net.Listen("tcp", address); err != nil {
			return err
		}
	}

	a.listenerShutdown.Add(1)
	go a.listenForConnections()
	return nil
}

//NewAcceptorDynamic creates and initializes a new AcceptorDynamic.
func NewAcceptorDynamic(app Application, storeFactory MessageStoreFactory, settings *Settings, logFactory LogFactory, verifier VerifyConnection) (a *AcceptorDynamic, err error) {
	a = &AcceptorDynamic{
		Acceptor: &Acceptor{
			app:          app,
			storeFactory: storeFactory,
			settings:     settings,
			logFactory:   logFactory,
			sessions:     make(map[SessionID]*session),
		},
		verifier: verifier,
	}

	if a.globalLog, err = logFactory.Create(); err != nil {
		return
	}

	return
}

func (a *AcceptorDynamic) listenForConnections() {
	defer a.listenerShutdown.Done()

	for {
		netConn, err := a.listener.Accept()
		if err != nil {
			return
		}

		go func() {
			a.handleConnection(netConn)
		}()
	}
}

func (a *AcceptorDynamic) handleConnection(netConn net.Conn) {
	defer func() {
		if err := recover(); err != nil {
			a.globalLog.OnEventf("Connection Terminated with Panic: %s", debug.Stack())
		}

		if err := netConn.Close(); err != nil {
			a.globalLog.OnEvent(err.Error())
		}
	}()

	reader := bufio.NewReader(netConn)
	parser := newParser(reader)

	msgBytes, err := parser.ReadMessage()
	if err != nil {
		if err == io.EOF {
			a.globalLog.OnEvent("Connection Terminated")
		} else {
			a.globalLog.OnEvent(err.Error())
		}
		return
	}

	msg := NewMessage()
	err = ParseMessage(msg, msgBytes)
	if err != nil {
		a.invalidMessage(msgBytes, err)
		return
	}

	var beginString FIXString
	if err := msg.Header.GetField(tagBeginString, &beginString); err != nil {
		a.invalidMessage(msgBytes, err)
		return
	}

	var senderCompID FIXString
	if err := msg.Header.GetField(tagSenderCompID, &senderCompID); err != nil {
		a.invalidMessage(msgBytes, err)
		return
	}

	var senderSubID FIXString
	if msg.Header.Has(tagSenderSubID) {
		if err := msg.Header.GetField(tagSenderSubID, &senderSubID); err != nil {
			a.invalidMessage(msgBytes, err)
			return
		}
	}

	var senderLocationID FIXString
	if msg.Header.Has(tagSenderLocationID) {
		if err := msg.Header.GetField(tagSenderLocationID, &senderLocationID); err != nil {
			a.invalidMessage(msgBytes, err)
			return
		}
	}

	var targetCompID FIXString
	if err := msg.Header.GetField(tagTargetCompID, &targetCompID); err != nil {
		a.invalidMessage(msgBytes, err)
		return
	}

	var targetSubID FIXString
	if msg.Header.Has(tagTargetSubID) {
		if err := msg.Header.GetField(tagTargetSubID, &targetSubID); err != nil {
			a.invalidMessage(msgBytes, err)
			return
		}
	}

	var targetLocationID FIXString
	if msg.Header.Has(tagTargetLocationID) {
		if err := msg.Header.GetField(tagTargetLocationID, &targetLocationID); err != nil {
			a.invalidMessage(msgBytes, err)
			return
		}
	}

	sessID := SessionID{BeginString: string(beginString),
		SenderCompID: string(targetCompID), SenderSubID: string(targetSubID), SenderLocationID: string(targetLocationID),
		TargetCompID: string(senderCompID), TargetSubID: string(senderSubID), TargetLocationID: string(senderLocationID),
	}

	if a.verifier != nil {
		err := a.verifier(msg, &sessID)
		if err != nil {
			a.globalLog.OnEventf("Connection verified failed: %s, %v", msg.String(), err.Error())
			return
		}
	}

	session, err := a.getOrCreateSession(sessID)
	if err != nil {
		a.globalLog.OnEventf(err.Error())
		return
	}

	msgIn := make(chan fixIn)
	msgOut := make(chan []byte)

	if err := session.connect(msgIn, msgOut); err != nil {
		a.globalLog.OnEventf("Unable to accept %v", err.Error())
		return
	}

	go func() {
		msgIn <- fixIn{msgBytes, parser.lastRead}
		readLoop(parser, msgIn)
	}()

	writeLoop(netConn, msgOut, a.globalLog)
}

func (a *AcceptorDynamic) getOrCreateSession(sessID SessionID) (*session, error) {
	a.sessLock.Lock()
	defer a.sessLock.Unlock()

	session, ok := a.sessions[sessID]
	if !ok {

		for _, sessionSettings := range a.settings.SessionSettings() {
			sessID.Qualifier = ""

			// Create this session
			var err error
			if a.sessions[sessID], err = a.createSession(sessID, a.storeFactory, sessionSettings, a.logFactory, a.app); err != nil {
				return nil, errors.New("create session error")
			}

			// Run this session
			session = a.sessions[sessID]
			a.sessionGroup.Add(1)
			go func() {
				session.run()
				a.sessionGroup.Done()
			}()

			break
		}
	}

	return session, nil
}
