echo "Started..."
git pull
pip install --quiet -U -r requirements.txt
python3 main.py
