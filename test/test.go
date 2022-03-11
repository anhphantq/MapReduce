package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
)

type A struct {
	X int
	Y string
}

var path = "/home/phananhtq/Documents/cs6824/MapReduce"

func main() {
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

	// err := os.Mkdir(path+"/reduce_data/woker01", 0755)
	// if err != nil {
	// 	log.Fatal(err.Error())
	// }
	file, err := ioutil.TempFile("", "*.txt")
	if err != nil {
		log.Fatal(err.Error(), "38")
	}

	//fmt.Println(os.TempDir())
	fmt.Println(file.Name())
	fmt.Println(path + "/reduce_data/" + "worker01" + "/" + filepath.Base(file.Name()))
	err = os.Rename(file.Name(), path+"/reduce_data/"+"worker01"+"/"+"1.txt")
	fmt.Print(err.Error())
}
