# ATV_Websocket

python -m venv venv
source venv/Scripts/activate
set -a && source .env && set +a
python -m uvicorn app.service:app --host localhost --port 8000 --reload
