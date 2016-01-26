package main

import (
	//"bytes"
	//"compress/gzip"
	"encoding/json"
	"flag"
	"github.com/boltdb/bolt"
	"github.com/gnewton/gomesh"
	"log"
)

var DESCRIPTOR_XML_FILE *string
var QUALIFIER_XML_FILE *string
var SUPPLEMENTAL_XML_FILE *string
var PHARMACOLOGICAL_XML_FILE *string

const BUCKET_SUPPLEMENTAL = "supplemental"

func init() {
	DESCRIPTOR_XML_FILE = flag.String("D", "testData/desc2014_29records.xml.bz2", "Full path to descriptor XML file")
	QUALIFIER_XML_FILE = flag.String("Q", "testData/qual2014_8records.xml.bz2", "Full path to qualifier XML file")
	SUPPLEMENTAL_XML_FILE = flag.String("S", "testData/supp2014_4records.xml", "Full path to supplemental XML file")
	PHARMACOLOGICAL_XML_FILE = flag.String("P", "testData/pa2014_8records.xml", "Full path to pharmacological supplemental XML file")

}

func main() {
	flag.Parse()

	db, err := bolt.Open("my.db", 0600, nil)
	if err != nil {
		log.Fatal(err)
	}

	tx, err := db.Begin(true)
	if err != nil {
		log.Fatal(err)
	}

	// Use the transaction...
	_, err = tx.CreateBucket([]byte(BUCKET_SUPPLEMENTAL))
	if err != nil {
		log.Fatal(err)
	}

	// Commit the transaction and check for error.
	if err := tx.Commit(); err != nil {
		log.Fatal(err)
	}

	log.Println("\tLoading Supplemental MeSH XML from file: ", *SUPPLEMENTAL_XML_FILE)

	suppChannel, file, err := gomesh.SupplementalChannelFromFile(*SUPPLEMENTAL_XML_FILE)
	if err != nil {
		log.Fatal(err)
	}
	commitSize := 10000
	commitCounter := 0
	tx, err = db.Begin(true)
	if err != nil {
		log.Fatal("error:", err)
	}
	b := tx.Bucket([]byte(BUCKET_SUPPLEMENTAL))
	for s := range suppChannel {
		if commitCounter == commitSize {
			if err := tx.Commit(); err != nil {
				log.Fatal(err)
			}
			tx, err = db.Begin(true)
			if err != nil {
				log.Fatal("error:", err)
			}
			b = tx.Bucket([]byte(BUCKET_SUPPLEMENTAL))
			commitCounter = 0
		} else {
			commitCounter = commitCounter + 1
		}

		key := s.SupplementalRecordUI
		value, err := json.Marshal(s)
		if err != nil {
			log.Fatal("error:", err)
		}
		if commitCounter == commitSize {
			log.Println(key)
			log.Println(len(value))
			// var b bytes.Buffer
			// w := gzip.NewWriter(&b)
			// w.Write(value)
			// w.Close()
			// log.Println(b.Len())
		}
		//log.Println(string(value))

		err = b.Put([]byte(key), value)
		if err != nil {
			log.Fatal(err)
		}

	}
	file.Close()
	if err := tx.Commit(); err != nil {
		log.Fatal(err)
	}

	suppChannel, file, err = gomesh.SupplementalChannelFromFile(*SUPPLEMENTAL_XML_FILE)
	if err != nil {
		log.Fatal(err)
	}
	// db.Close()

	// db, err = bolt.Open("my.db", 0600, nil)
	// if err != nil {
	// 	log.Fatal(err)
	// }

	log.Println("")
	log.Println("")
	log.Println("")

	log.Println("****************************************************************************************")
	log.Println("****************************************************************************************")
	log.Println("****************************************************************************************")
	tx, err = db.Begin(false)
	if err != nil {
		log.Fatal("error:", err)
	}

	suppChannel, file, err = gomesh.SupplementalChannelFromFile(*SUPPLEMENTAL_XML_FILE)
	if err != nil {
		log.Fatal(err)
	}
	b = tx.Bucket([]byte(BUCKET_SUPPLEMENTAL))

	// c := b.Cursor()
	// for k, _ := c.First(); k != nil; k, _ = c.Next() {
	// 	log.Printf("key=%s\n", k)
	// }
	log.Println("****************************************************************************************")
	for s := range suppChannel {
		key := s.SupplementalRecordUI

		v := b.Get([]byte(key))
		if v == nil {
			log.Println("Unable to find", key)
		}
	}
}
