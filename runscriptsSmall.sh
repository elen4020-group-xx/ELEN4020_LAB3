clear

echo "Small File"

echo "WordCount"
python wordcount.py File1ForLab3.txt
echo "Top 10"
python kMost.py File1ForLab3.txt --maxNo 10
echo "Top 20"
python kMost.py File1ForLab3.txt --maxNo 20

echo "Inverted-General"
python reverseindex.py File1ForLab3.txt
$SHELL

