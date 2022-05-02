from distutils.command.build_scripts import first_line_re
from tokenize import group
from pymongo import MongoClient
import pprint

client = MongoClient("mongodb://root:pass12345@127.0.0.1:27017") 
db = client["DBLP"] 
publis = db["publis"] 
documents = publis.find()

# Compter le nombre de documents chargé depuis le JSON
count=0
for document_lu in documents:
    count+=1
print('\nIl y a', count, 'publications dans la base. \n')

#Afficher toutes les publications de type livre
books =publis.find({'type' :'Book'})
for books_lu in books :
    print(books_lu,'\n')

#Afficher les publications de type livre depuis 2014
publis_2014 = publis.find({ 'year' : {'$gte': 2014 }, 'type' :'Book'} )
for publis_2014_lus in publis_2014 :
    print(publis_2014_lus)

# Afficher les publications écrites par Toru Ishisa
publis_ishida = publis.find({ 'authors' : 'Toru Ishida'} )
count_ishida=0
for publis_ishida_lus in publis_ishida :
    count_ishida+=1
    print(publis_ishida_lus)

# Afficher le nombre de publications de Ishida
print("Toru Ishida a publié", count_ishida,"fois")

# Trier par titre les publications de Toru Ishida
for doc in publis.find({'authors' : 'Toru Ishida'}).sort('title'):
    print(doc)

# Lister les auteurs distinctement
print(db.publis.distinct("authors"))

# # Compter le nombre de publications depuis 2011 et par type
count=0
for publi_book in publis.find({ 'year' : {'$gte': 2011 }, 'type' :'Book'}):
    count+=1
print("Il y a",count,'livres écrits depuis 2011.')
count=0
for publi_art in publis.find({ 'year' : {'$gte': 2011 }, 'type' :'Article'}):
    count+=1
print("Il y a",count,'articles écrits depuis 2011.')

# Compter le nombre de publications par auteurs et les classer
for publis in db.publis.aggregate([{"$unwind" : "$authors"}, {"$sortByCount" : "$authors"}]):
    print(publis)