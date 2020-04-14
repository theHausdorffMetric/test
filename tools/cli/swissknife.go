// Cli tool to generate boring spider Boilerplate and harmonize structure.
//
// Lacking an available standalone binary, you will obviously need Go to build it.
//
//			de ./tools && go build -o swissknife
//
// usage:
//
//			./tools/swissknife -name=MarineTraffic -doc="scrape MT" -url=www.mt.com
package main

import (
	"flag"
	"fmt"
	"html/template"
	"os"
)

const spiderTemplate = `
# -*- coding: utf-8 -*-

"""{{ .Name }}.

{{ .Doc }}

"""

import scrapy


class {{ .Name }}Spider(scrapy.Spider):

    name = '{{ .Name }}'
    start_urls = [ {{ .URL }} ]

    def parse(self, response):
        print(response.body)
`

// Spider struct holds required information one needs to provide to build a
// well-being spider
type Spider struct {
	Name string
	Doc  string
	URL  string
}

// TODO `-type ais` and import AISSpider
func main() {
	spiderName := flag.String("name", "", "spider name")
	spiderDoc := flag.String("doc", "", "spider doc")
	// TODO support multiple urls
	spiderURL := flag.String("url", "www.example.com", "start url to scrape")

	// TODO command (flags.Args()[0])

	flag.Parse()

	if *spiderName == "" {
		fmt.Printf("error: a spider needs a name\n")
		os.Exit(1)
	}

	spider := &Spider{
		Name: *spiderName,
		Doc:  *spiderDoc,
		URL:  *spiderURL,
	}

	fmap := template.FuncMap{}
	t, err := template.New("spider").Funcs(fmap).Parse(spiderTemplate)
	if err != nil {
		panic(err)
	}

	if err := t.Execute(os.Stdout, spider); err != nil {
		panic(err)
	}
}
