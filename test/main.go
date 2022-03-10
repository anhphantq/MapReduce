package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
)

type A struct{
	X int 
	Y string
}

func main(){
	// tmpfile, err := ioutil.TempFile("", "example")

	// if err != nil {
	// 	log.Fatal(err)
	// }
	 
	// enc := json.NewEncoder(tmpfile)
	// enc.Encode(&A{X: 0, Y: "init"})

	// for i := 1; i <= 3; i++{
	// 	enc.Encode(&A{X: i, Y: "sos"})
	// }

	// os.Rename(tmpfile.Name(), fmt.Sprintf("mr-%d.txt", 1))

	var kva []A

	file, err := os.Open("mr-1.txt")
	fmt.Println(file.Name())
	if (err != nil){
		log.Fatal("ga")
	}
	dec := json.NewDecoder(file)
  	for {
    	var kv A
    	if err := dec.Decode(&kv); err != nil {
      		break
    	}
		fmt.Print(kv.X, kv.Y)
    	kva = append(kva, kv)
  	}

	fmt.Println(kva[2].X, kva[2].Y + "khok")
}