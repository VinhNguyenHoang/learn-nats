package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"

	pb "github.com/VinhNguyenHoang/learn-grpc/chatserver"
	"github.com/jackc/pgx/v4/pgxpool"
	"google.golang.org/grpc"
)

var (
	userId = flag.Int("userId", 0, "User ID ")
	serverId = flag.Int("serverId", 1, "ID of server to connect")
)

type client struct {
	ctx context.Context
	db *pgxpool.Pool
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

func NewClient() *client {
	c := &client{
		ctx: context.Background(),
	}
	c.db = InitDb(c.ctx)

	return c
}

func (c *client) Close() {
	c.db.Close()
}

func (c *client) GetServerURL() string {
	var url string
	err := c.db.QueryRow(c.ctx, "SELECT s_url FROM servers WHERE s_id = $1", serverId).Scan(&url)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to retrieve server info from database: %v\n", err)
		os.Exit(1)
	}
	return url
}

// input string will have this format
// "id=12 msg=hello world"
func parseInput(input string) (userId int32, message string) {
	idIdx := strings.Index(input, "id")
	idOffset := 3
	spcIdx := strings.Index(input[idIdx+idOffset:], " ")
	if spcIdx == -1 {
		spcIdx = 1
	}
	id, err := strconv.ParseInt(input[idIdx+idOffset:idIdx+idOffset+spcIdx], 10, 64)
	if err != nil {
		fmt.Println("Error parsing input", err)
	}
	msg := input[idIdx+idOffset+spcIdx+5:]

	return int32(id), msg
}

func main() {
	flag.Parse()
	client := NewClient()

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt)
	go func() {
		<-sigs
		client.Close()
		os.Exit(0)
	}()

	conn, err := grpc.Dial(client.GetServerURL(), grpc.WithInsecure())
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()

	clientChat := pb.NewChatServerClient(conn)
	stream, err := clientChat.Subscribe(ctx, &pb.SubscribeRequest{UserId: int32(*userId)})
	if err != nil {
		log.Fatal(err)
	}

	scanner := bufio.NewScanner(os.Stdin)
	done := make(chan bool)
	go func() {
		for {
			response, err := stream.Recv()
			if err == io.EOF {
				done <- true
				return
			}
			if err != nil {
				log.Fatal(err)
			}
			fmt.Println(response.FromUserId , " says " , response.Message)
		}
	}()
	fmt.Println("Connected to chat server. Press Ctrl + C to exit.")
	for scanner.Scan() {
		input := scanner.Text()
		toUserId, message := parseInput(input)
		clientChat.Send(ctx, &pb.ChatRequest{FromUserId: int32(*userId), ToUserId: toUserId, Message: message})
	}
	<- done
}