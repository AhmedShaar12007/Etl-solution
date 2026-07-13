@echo off
echo Installing dependencies...
pip install -r requirements.txt --quiet

echo.
echo Starting Pipeline Control Center...
echo Open your browser at: http://localhost:5050
echo.
python main.py
pause
