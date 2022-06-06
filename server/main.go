package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"

	pb "github.com/VinhNguyenHoang/learn-grpc/chatserver"
	"github.com/jackc/pgx/v4"
	pgxpool "github.com/jackc/pgx/v4/pgxpool"
	"github.com/nats-io/nats.go"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

var (
	serverId = flag.Int("serverId", 1, "Server Id")
	serverUrl = flag.String("serverUrl", "localhost:50079", "URL of server")

	// nats
	streamName = "CHAT"
	streamSubjects = "CHAT.*"
	subjectName = flag.String("sub", "", "subject name")
	subjectNameTo = flag.String("subTo", "", "subject to receive msg")
)

type server struct{
	pb.UnimplementedChatServerServer
	UserList map[int]chan *pb.ChatRequest
	db *pgxpool.Pool
	ctx context.Context
	natConn *nats.Conn
	js nats.JetStreamContext
}

func (s *server) Subscribe(input *pb.SubscribeRequest, stream pb.ChatServer_SubscribeServer) error {
	// if UserId is not in the Map list, add UserId with pb SubscribeClient
	ch := make(chan *pb.ChatRequest)
	if _, ok := s.UserList[int(input.UserId)]; !ok {
		s.UserList[int(input.UserId)] = ch

		// insert connection to DB
		s.InsertConnection(*serverId, int(input.UserId))

		fmt.Println("Server.Subscribed user ", input.UserId)
	}
	// listen for disconnection or chat request from User
	done := make(chan bool)
	go func() {
		for {
			select {
			case req := <-ch:
				stream.Send(&pb.ChatResponse{FromUserId: req.FromUserId, Message: req.Message})
			case <- stream.Context().Done():
				delete(s.UserList, int(input.UserId))
				s.DeleteConnection(*serverId, int(input.UserId))
				fmt.Println("Connection to UserID", input.UserId, "has ended")
				done <- true
			}
		}
	}()
	<- done

	return nil
}

func (s *server) Send(ctx context.Context, req *pb.ChatRequest) (*pb.EmptyResponse, error) {
	if req.ToUserId != 0 {
		// ToUserId is exist in the current Server
		if c, ok := s.UserList[int(req.ToUserId)]; ok {
			if len(req.Message) > 0 {
				c <- req // pass the ChatRequest to the ToUserId's channel to stream message to that user
			}
		} else { // if not, look for ToUserId in DB to see if user is connected to other server
			//s.GetServerUserIsConnecting(int(req.ToUserId)) // get server ID

			//TODO send message to NATS to send message to ToUserId
			payload, err := proto.Marshal(req)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Println("Publishing msg to", *subjectNameTo)
			s.js.Publish(*subjectNameTo, payload)
		}
	}
	return &pb.EmptyResponse{}, nil
}

func (s *server) InsertConnection(serverId, userId int) {
	_, err := s.db.Exec(s.ctx, "INSERT INTO connections VALUES($1, $2)", serverId, userId)
	if err != nil {
		log.Fatal(err)
	}
}

func (s *server) GetServerUserIsConnecting(userId int) int {
	var result int
	err := s.db.QueryRow(s.ctx, "SELECT s_id FROM connections WHERE c_id = $1", userId).Scan(&result)
	if err != pgx.ErrNoRows {
		return result
	}
	return 0
}

func (s *server) DeleteConnection(serverId, userId int) {
	_, err := s.db.Exec(s.ctx, "DELETE FROM connections WHERE s_id = $1 AND c_id = $2", serverId, userId)
	if err != nil {
		log.Fatal(err)
	}
}

func (s *server) Close() {
	fmt.Println("Cleaning up server...")
	s.UnRegisterServerToDb()
	s.db.Close()
}

func InitDb(c context.Context) *pgxpool.Pool {
	dbURL := "postgres://postgres:password@localhost:30432/chat"
	conn, err := pgxpool.Connect(c, dbURL)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to connect to database: %v\n", err)
		os.Exit(1)
	}

	return conn
}

func NewServer() *server {
	s := &server{
		UserList: make(map[int]chan *pb.ChatRequest),
		ctx: context.Background(),
	}

	s.db = InitDb(s.ctx)
	natConn, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		log.Fatal(err)
	}
	s.natConn = natConn

	s.js, _ = s.natConn.JetStream()

	_, err = s.js.StreamInfo(streamName)
	if err == nats.ErrStreamNotFound {
		_, err = s.js.AddStream(&nats.StreamConfig{
			Name: streamName,
			Subjects: []string{streamSubjects},
		})
		if err != nil {
			log.Fatal(err)
		}
	}

	return s
}

func checkDbConnections(s *server) {
	rows, err := s.db.Query(s.ctx, "select count(*) from connections")
	if err != nil {
		fmt.Fprintf(os.Stderr, "QueryRow failed: %v\n", err)
		os.Exit(1)
	}
	defer rows.Close()
	var result int
	rows.Scan(&result)
	fmt.Println("There are", result, "connections to server")
}

func (s *server) RegisterServerToDb() {
	_, err := s.db.Exec(s.ctx, "INSERT INTO servers VALUES($1, $2)", serverId, serverUrl)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to register Server Info to database: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Registered server %d address %s to database\n", *serverId, *serverUrl)
}

func (s *server) UnRegisterServerToDb() {
	_, err := s.db.Exec(s.ctx, "DELETE FROM servers WHERE s_id = $1", serverId)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to unregister Server Info to database: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Unregistered server %d from database\n", *serverId)
}

func main() {
	flag.Parse()

	server := NewServer()

	server.RegisterServerToDb()

	go func() {
		server.js.Subscribe(*subjectName, func(m *nats.Msg){
			fmt.Printf("Received a JetStream message: %s\n", string(m.Data))
			msg := &pb.ChatRequest{}
			proto.Unmarshal(m.Data, msg)
			server.Send(server.ctx, msg)
		})
	}()

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt)
	go func() {
		<-sigs
		server.Close()
		os.Exit(0)
	}()

	checkDbConnections(server)

	lis, err := net.Listen("tcp", *serverUrl)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterChatServerServer(s, server)
	log.Printf("server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}