@echo off
echo Starting Portfolio Server...
echo.
echo Choose a server option:
echo 1. Python Server (recommended)
echo 2. Node.js Server
echo.
set /p choice="Enter your choice (1 or 2): "

if "%choice%"=="1" (
    echo.
    echo Starting Python server...
    python server.py
) else if "%choice%"=="2" (
    echo.
    echo Starting Node.js server...
    node server.js
) else (
    echo.
    echo Invalid choice. Please run the batch file again and select 1 or 2.
    pause
) 