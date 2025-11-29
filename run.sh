#!/bin/bash
# Install dependencies if needed
# pip install -r requirements.txt

# Run the server
uvicorn server:app --host 0.0.0.0 --port 8000 --reload
