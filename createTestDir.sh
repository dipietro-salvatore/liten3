#!/usr/bin/env bash

DIR="testDir"

rm -r $DIR
mkdir $DIR
cd $DIR

# Create Files
for i in $(seq 1 20)
do
    head -c $RANDOM /dev/urandom > file_${i}.txt
done
touch nullFile.txt

# Create Folder
mkdir folder1

# Create files in folder
for i in $(seq 1 6)
do
    head -c $RANDOM /dev/urandom > folder1/f1file_${i}.txt
done

cp -r folder1 folder2 # Duplicate Files
rename 's/f1/f2/' folder2/*.*

for i in $(seq 7 10)
do
    head -c $RANDOM /dev/urandom > folder1/f1file_${i}.txt
done
for i in $(seq 7 10)
do
    head -c $RANDOM /dev/urandom > folder2/f2file_${i}.txt
done
touch folder2/nullFile.txt


# Create Links

ln file_1.txt hardlink_file_1.txt
ln file_3.txt hardlink_file_3.txt
ln -s file_5.txt symblink_file_5.txt
ln -s file_7.txt symblink_file_7.txt


# Show DIR content
find . -print0 | xargs -0 ls -al

