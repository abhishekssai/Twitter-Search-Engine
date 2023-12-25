#! usr/bin/bash

echo "Building index on twitter data"

if [ $# -eq 0 ]
then
	echo "Proceeding with default data file [data.jsonl]"
	python3 build_index.py
else
	echo "Building index on" $1
	python3 build_index.py -D $1
fi

if  [ $? -eq 0 ]; then
	echo "Indexing completed."
	python3 search_index.py
else
	echo "Indexing failed. Please use individual scripts to build index."
fi

