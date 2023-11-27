package main

// func main() {
// 	ctx, cancel := context.WithTimeout(context.Background(), time.Second)

// 	defer cancel()

// 	slowFunc := func(ctx context.Context, i int) {
// 		childCtx, cancel := context.WithTimeout(ctx, 800*time.Millisecond)
// 		defer cancel()

// 		fmt.Printf("query No. %d\n", i)
// 		select {
// 		case <-childCtx.Done():
// 			fmt.Printf("child context err: %v\n", childCtx.Err())
// 		}
// 	}

// 	select {
// 	case <-ctx.Done():
// 		fmt.Printf("parent context err: %v\n", ctx.Err())
// 		return
// 	default:
// 		for i := 0; i < 5; i++ {
// 			slowFunc(ctx, i)
// 		}
// 	}
// }

// func main() {
// 	messages := make(chan int, 10)
// 	done := make(chan bool)
// 	defer close(messages)

// 	// consumer
// 	go func() {
// 		ticker := time.NewTicker(time.Second)
// 		for _ = range ticker.C {
// 			select {
// 			case <-done:
// 				fmt.Println("child process end....")
// 				return
// 			default:
// 				fmt.Printf("send message : %d\n", <-messages)
// 			}
// 		}
// 	}()

// 	// provider
// 	for i := 0; i < 10; i++ {
// 		messages <- i
// 	}
// 	time.Sleep(5 * time.Second)
// 	close(done)
// 	time.Sleep(1 * time.Second)
// 	fmt.Println("main process exit!")
// }

// func main() {
// 	messages := make(chan int, 10)
// 	// producer
// 	for i := 0; i < 10; i++ {
// 		messages <- i
// 	}

// 	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

// 	// consumer
// 	go func(ctx context.Context) {
// 		ticker := time.NewTicker(time.Second)
// 		for _ = range ticker.C {
// 			select {
// 			case <-ctx.Done():
// 				fmt.Println("child process interrupt....")
// 				return
// 			default:
// 				fmt.Printf("send message : %d\n", <-messages)
// 			}
// 		}
// 	}(ctx)

// 	defer close(messages)
// 	defer cancel()

// 	select {
// 	case <-ctx.Done():
// 		time.Sleep(time.Second)
// 		fmt.Println("main process exit!")
// 	}
// }

// func main() {
// 	ech := make(chan int)
// 	time.AfterFunc(5*time.Second, func() {
// 		ech <- 2
// 	})

// 	// wait for handler to return,
// 	// but stop waiting if DeleteServer() has been called,
// 	// and return an error.
// 	fmt.Println("start wait reply-----")
// 	var reply int
// 	replyOK := false
// 	serverDead := false
// 	for replyOK == false && serverDead == false {
// 		select {
// 		case reply = <-ech:
// 			replyOK = true
// 		case <-time.After(10 * time.Second):
// 			serverDead = true
// 			if serverDead {
// 				go func() {
// 					<-ech // drain channel to let the goroutine created earlier terminate
// 				}()
// 			}
// 		}
// 	}

// 	fmt.Println(reply)
// }

// func main() {
// 	for i := 0; i < 5; i++ {
// 		defer func(num int) {
// 			fmt.Printf("%d\n", num)
// 		}(i)
// 	}
// 	fmt.Println("end...")
// }
