clear

echo "Big File"

echo "WordCount"
python wordcount.py File2ForLab3.txt
echo "Top 10"
python kMost.py File2ForLab3.txt --maxNo 10
echo "Top 20"
python kMost.py File2ForLab3.txt --maxNo 20

echo "Inverted-General"
python reverseindex.py File2ForLab3.txt
echo "Inverted-Specific"
python reverseAlternative.py File2ForLab3.txt
$SHELL
