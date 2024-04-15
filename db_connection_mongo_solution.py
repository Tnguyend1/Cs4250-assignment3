#-------------------------------------------------------------------------
# AUTHOR: Thoa Nguyen
# FILENAME: db_connection_mongo_solution.py
# SPECIFICATION: practice mongodb 
# FOR: CS 4250- Assignment #3
# TIME SPENT: 3
#-----------------------------------------------------------*/

#IMPORTANT NOTE: DO NOT USE ANY ADVANCED PYTHON LIBRARY TO COMPLETE THIS CODE SUCH AS numpy OR pandas. You have to work here only with
# standard arrays

#importing some Python libraries
from pymongo import *;
import datetime

def connectDataBase():

    # Create a database connection object using pymongo
    client = MongoClient('mongodb://localhost:27017/')
    db = client['inverted_index_db']
    return db['documents']

def createDocument(col, docId, docText, docTitle, docDate, docCat):

    # create a dictionary indexed by term to count how many times each term appears in the document.
    # Use space " " as the delimiter character for terms and remember to lowercase them.
    term_counts = {}
    terms = docText.lower().split(" ")
    for index, term in enumerate(terms):
        re_Term = "".join(char for char in term if char.isalnum())
        terms[index] = re_Term
        term_counts[re_Term] = term_counts.get(re_Term, 0) + 1

    # create a list of objects to include full term objects. [{"term", count, num_char}]
    term_objects = []
    for term in set(terms):  # Remove repetitions
        term_objects.append({
            "term": term,
            "num_chars": len(term),
            "count": term_counts[term]
        })
    numDocChars = sum(len(term) for term in terms)
    # produce a final document as a dictionary including all the required document fields
    document = {
        "_id": int(docId),
         "text": docText,
        "title": docTitle,
        "num_chars": numDocChars,
        "date": datetime.datetime.strptime(docDate, "%Y-%m-%d"),
        "category": docCat,
        "terms": term_objects
    }
    # insert the document
    col.insert_one(document)

def deleteDocument(col, docId):

    # Delete the document from the database
    col.delete_one({'_id': int(docId)})
def updateDocument(col, docId, docText, docTitle, docDate, docCat):

    # Delete the document
    deleteDocument(col, docId)
    # Create the document with the same id
    createDocument(col, docId, docText, docTitle, docDate, docCat)
def getIndex(col):

    # Query the database to return the documents where each term occurs with their corresponding count. Output example:
    # {'baseball':'Exercise:1','summer':'Exercise:1,California:1,Arizona:1','months':'Exercise:1,Discovery:3'}
    # ...
    invertIndexes = {}

    # Iterate through each document in the collection
    for doc in col.find():
        terms = doc['terms']
        for termObj in terms:
            term = termObj['term']
            count = termObj['count']
            title = doc['title']

            if term in invertIndexes:
                invertIndexes[term][title] = count
            else:
                invertIndexes[term] = {title: count}

    # Sort the inverted index alphabetically by terms and documents
    sortedInvertIndexes = sorted(invertIndexes.items())
    for term, index in sortedInvertIndexes:
        sortedIndex = sorted(index.items())
        invertIndexes[term] = ", ".join([f"{doc}:{count}" for doc, count in sortedIndex])

    return invertIndexes
