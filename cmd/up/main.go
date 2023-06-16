package main

import (
	"flag"
	operation2 "github.com/service-sdk/go-sdk-qn/v2/operation"
	"log"
)

func main() {
	cf := flag.String("c", "cfg.toml", "config")
	f := flag.String("f", "file", "upload file")
	flag.Parse()

	x, err := operation2.Load(*cf)
	if err != nil {
		log.Fatalln(err)
	}

	up := operation2.NewUploader(x)

	err = up.Upload(*f, *f)
	log.Fatalln(err)
}
