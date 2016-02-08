package main

import (
	//"bytes"
	//"compress/gzip"
	"encoding/json"
	"flag"
	"github.com/boltdb/bolt"
	"github.com/gnewton/gomesh"
	"log"
	"strings"
)

// Write MeSH XMl for MeSH 2014
// Glen Newton
// Copyright 2016
//

const MESH_VERSION = 2014
const BUCKET_DESCRIPTOR = "descriptor"
const BUCKET_PHARMACOLOGICAL = "pharmacological"
const BUCKET_QUALIFIER = "qualifier"
const BUCKET_SUPPLEMENTAL = "supplemental"

var descriptorXmlFile *string
var qualifierXmlFile *string
var supplementalXmlFile *string
var pharmacologicalXmlFile *string
var dbFile *string
var commitSize = 10000

func init() {
	descriptorXmlFile = flag.String("D", "testData/desc2014_29records.xml.bz2", "Full path to descriptor XML file")
	qualifierXmlFile = flag.String("Q", "testData/qual2014_8records.xml.bz2", "Full path to qualifier XML file")
	supplementalXmlFile = flag.String("S", "testData/supp2014_4records.xml", "Full path to supplemental XML file")
	pharmacologicalXmlFile = flag.String("P", "testData/pa2014_8records.xml", "Full path to pharmacological supplemental XML file")
	dbFile = flag.String("f", "mesh.bolt", "bolt db file to be written to")
}

func main() {
	log.Println("MeSH version: ", MESH_VERSION)
	flag.Parse()

	db, err := bolt.Open(*dbFile, 0600, nil)
	if err != nil {
		log.Fatal(err)
	}

	loadDescriptor(db)
	loadSupplemental(db)

	loadQualifier(db)
	loadPharmacological(db)

	db.Close()

}

func loadDescriptor(db *bolt.DB) {
	tx, err := db.Begin(true)
	if err != nil {
		log.Fatal(err)
	}

	// Use the transaction...
	_, err = tx.CreateBucket([]byte(BUCKET_DESCRIPTOR))
	if err != nil {
		log.Fatal(err)
	}

	// Commit the transaction and check for error.
	if err := tx.Commit(); err != nil {
		log.Fatal(err)
	}

	log.Println("\tLoading Description MeSH XML from file: ", *descriptorXmlFile)

	descChannel, file, err := gomesh.DescriptorChannelFromFile(*descriptorXmlFile)
	if err != nil {
		log.Fatal(err)
	}

	commitCounter := 0
	counter := 0
	tx, err = db.Begin(true)
	if err != nil {
		log.Fatal("error:", err)
	}
	b := tx.Bucket([]byte(BUCKET_DESCRIPTOR))

	size := 0

	root := InitializeNode()

	for desc := range descChannel {
		for _, tree := range desc.TreeNumberList.TreeNumber {
			root.AddNode(tree.TreeNumber, desc.DescriptorUI, desc.DescriptorName)
			//log.Println("---------")
			//log.Println(tree)
			m := strings.Split(tree.TreeNumber, ".")
			if len(m) > size {
				size = len(m)
			}
		}
		counter = counter + 1
		if commitCounter == commitSize {
			if err := tx.Commit(); err != nil {
				log.Fatal(err)
			}
			tx, err = db.Begin(true)
			if err != nil {
				log.Fatal("error:", err)
			}
			b = tx.Bucket([]byte(BUCKET_DESCRIPTOR))
			commitCounter = 0
		} else {
			commitCounter = commitCounter + 1
		}

		key := desc.DescriptorUI
		value, err := json.Marshal(desc)
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
	log.Println("Loaded", counter, "description")
	log.Println("Size=", size)
	root.DepthTraverse(0, Visitor)
}

func Visitor(n *Node) {
	log.Println("**  Visited", n.TreeNumber)
}

func loadQualifier(db *bolt.DB) {
	tx, err := db.Begin(true)
	if err != nil {
		log.Fatal(err)
	}

	// Use the transaction...
	_, err = tx.CreateBucket([]byte(BUCKET_QUALIFIER))
	if err != nil {
		log.Fatal(err)
	}

	// Commit the transaction and check for error.
	if err := tx.Commit(); err != nil {
		log.Fatal(err)
	}

	log.Println("\tLoading Qualifier MeSH XML from file: ", *qualifierXmlFile)

	qualChannel, file, err := gomesh.QualifierChannelFromFile(*qualifierXmlFile)
	if err != nil {
		log.Fatal(err)
	}

	commitCounter := 0
	counter := 0
	tx, err = db.Begin(true)
	if err != nil {
		log.Fatal("error:", err)
	}
	b := tx.Bucket([]byte(BUCKET_QUALIFIER))
	for qualifier := range qualChannel {
		counter = counter + 1
		if commitCounter == commitSize {
			if err := tx.Commit(); err != nil {
				log.Fatal(err)
			}
			tx, err = db.Begin(true)
			if err != nil {
				log.Fatal("error:", err)
			}
			b = tx.Bucket([]byte(BUCKET_PHARMACOLOGICAL))
			commitCounter = 0
		} else {
			commitCounter = commitCounter + 1
		}

		key := qualifier.QualifierUI
		value, err := json.Marshal(qualifier)
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
	log.Println("Loaded", counter, "quality")

}

func loadPharmacological(db *bolt.DB) {
	tx, err := db.Begin(true)
	if err != nil {
		log.Fatal(err)
	}

	// Use the transaction...
	_, err = tx.CreateBucket([]byte(BUCKET_PHARMACOLOGICAL))
	if err != nil {
		log.Fatal(err)
	}

	// Commit the transaction and check for error.
	if err := tx.Commit(); err != nil {
		log.Fatal(err)
	}

	log.Println("\tLoading Pharmacological MeSH XML from file: ", *pharmacologicalXmlFile)

	pharmaChannel, file, err := gomesh.PharmacologicalChannelFromFile(*pharmacologicalXmlFile)
	if err != nil {
		log.Fatal(err)
	}

	commitCounter := 0
	counter := 0
	tx, err = db.Begin(true)
	if err != nil {
		log.Fatal("error:", err)
	}
	b := tx.Bucket([]byte(BUCKET_PHARMACOLOGICAL))
	for pharma := range pharmaChannel {
		counter = counter + 1
		if commitCounter == commitSize {
			if err := tx.Commit(); err != nil {
				log.Fatal(err)
			}
			tx, err = db.Begin(true)
			if err != nil {
				log.Fatal("error:", err)
			}
			b = tx.Bucket([]byte(BUCKET_PHARMACOLOGICAL))
			commitCounter = 0
		} else {
			commitCounter = commitCounter + 1
		}

		key := pharma.DescriptorReferredTo.DescriptorUI
		value, err := json.Marshal(pharma)
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
	log.Println("Loaded", counter, "pharma")
}

func loadSupplemental(db *bolt.DB) {
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

	log.Println("\tLoading Supplemental MeSH XML from file: ", *supplementalXmlFile)

	suppChannel, file, err := gomesh.SupplementalChannelFromFile(*supplementalXmlFile)
	if err != nil {
		log.Fatal(err)
	}

	commitCounter := 0
	counter := 0
	tx, err = db.Begin(true)
	if err != nil {
		log.Fatal("error:", err)
	}
	b := tx.Bucket([]byte(BUCKET_SUPPLEMENTAL))
	for s := range suppChannel {
		counter = counter + 1
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
	log.Println("Loaded", counter, "supplemental")

}
