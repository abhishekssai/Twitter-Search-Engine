from query_lucene import search_lucene
from query_bert import search_bert

while True:
    print("Choose option to search")
    print("1. Lucene")
    print("2. Bert")

    inp = int(input())
    if inp == 1:
        print(search_lucene(input("Enter your query for lucene : ")))
    elif inp == 2:
        print(search_bert(input("Enter your query for Bert : "), 10))
    else:
        break

