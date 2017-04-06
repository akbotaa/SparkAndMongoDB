import xml.sax
from xml.sax import handler
import os, re
import pandas as pd
from pymongo import MongoClient
import sys


client = MongoClient()
db = client['dblp']

os.chdir("/Users/akbota/Documents/fall16/BigData/SparkAndMongoDB")

class HwHandler( xml.sax.ContentHandler ):
    def __init__(self):
        self.CurrentData = ""
        self.bool = False
        self.author = ""
        self.authors = list()
        self.title = ""
        self.jb = ""       #either journal or booktitle
        self.year = ""
        self.pubkey = ""
        self.count_i = 0
        self.count_a = 0
        self.booklist = []
        self.artlist = []

# Call when an element starts
    def startElement(self, tag, attributes):
        self.CurrentData = tag

        if tag == "article" or tag == "inproceedings":
            self.bool = True
            self.pubkey = attributes["key"]

        elif tag=="author":
            self.author = ""
            
# Call when a character is read
    def characters(self, content):
        if self.bool == True:
            if self.CurrentData == "title":
                self.title += content
            
            elif self.CurrentData == "author":
                self.author += content

            elif self.CurrentData == "journal" or self.CurrentData == "booktitle":
                self.jb += content
            
            elif self.CurrentData == "year":
                self.year += content
                        
# Call when an elements ends
    def endElement(self, tag):
        
        #if end of xml file is reached, insert remaining records
        if tag == "dblp":
            
            db.Article.insert_many(self.artlist)
            db.Inproceedings.insert_many(self.booklist)
        
        if self.bool == True:
            self.CurrentData=""
            if tag == "author":
                
                #remove duplicate author names in a paper
                if self.author in self.authors:
                    print("Duplicate author found!!!!!!")
                else:
                    self.authors.append(self.author)

                
            elif tag == "article":
                self.count_a += 1
                    
                #deal with empty entries
                if self.jb=="":
                    self.jb=None
                if self.title=="":
                    self.title=None
                if self.year=="":
                    self.year=None
                
                #save a record as a dictionary
                info = {
                    "_id": self.pubkey,
                    "title": self.title,
                    "authors": self.authors,
                    "journal": self.jb,
                    "year": self.year
                }
                    
                self.artlist.append(info)   #store a record in a list of dictionaries
                
                #insert every 2000 records to mongodb collectively
                if self.count_a % 2000 == 0:
                    
                    db.Article.insert_many(self.artlist)
                    
                    self.artlist = []
                    
                    print("A: " + str(self.count_a))

                    
                self.CurrentData = ""
                self.bool = False
                self.title = ""
                self.jb = ""       #either journal or booktitle
                self.year = ""
                self.author = ""
                self.authors = list()
                
            elif tag == "inproceedings":
                self.count_i += 1
                    
                #deal with empty entries
                if self.jb=="":
                    self.jb=None
                if self.title=="":
                    self.title=None
                if self.year=="":
                    self.year=None
                
                #save a record as a dictionary
                info = {
                    "_id": self.pubkey,
                    "title": self.title,
                    "authors": self.authors,
                    "booktitle": self.jb,
                    "year": self.year
                }
                    
                self.booklist.append(info)   #store a record in a list of dictionaries
                
                #insert every 2000 records to mongodb collectively
                if self.count_i % 2000 == 0:
                    
                    db.Inproceedings.insert_many(self.booklist)
                    
                    self.booklist = []
                    
                    print("I: " + str(self.count_i))
                
        
                self.CurrentData = ""
                self.bool = False
                self.title = ""
                self.jb = ""       #either journal or booktitle
                self.year = ""
                self.author = ""
                self.authors = list()
            
            

parser = xml.sax.make_parser()

Handler = HwHandler()

parser.setContentHandler(Handler)
parser.parse(open('dblp-2015-12-01.xml'))

#close the database
client.close()