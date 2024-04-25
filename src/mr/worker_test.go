package mr

import (
	"fmt"
	"testing"
)

func TestWorker(t *testing.T) {
	ch := make(chan int, 10)
	for i := 0; i < 10; i++ {
		ch <- i
	}
	fmt.Println(len(ch))
	for i := 0; i < 10; i++ {
		<-ch
	}
	fmt.Println(len(ch))
	<-ch
}
