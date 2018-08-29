package utils

import (
	"testing"
	"fmt"
)

func TestFoo1(t *testing.T) {
	var a = "a1"
	c := make(chan bool, 1)
	func() {
		go func() { a = "hello" }()
		fmt.Println(a)
		c <- true
	}()

	<-c
}

func TestFoo2(t *testing.T) {
	var c = make(chan int,1)
	var a = "xx"

	f := func() {
		a = "hello, world"
		<-c
	}

	go f()
	c <- 1
	c <- 1
	fmt.Println(a)

}
