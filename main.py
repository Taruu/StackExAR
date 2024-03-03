from app import app
import webbrowser
import uvicorn

if __name__ == "__main__":
    webbrowser.open("0.0.0.0:8000/docs", new=0, autoraise=True)
    uvicorn.run(app, host="0.0.0.0", port=8000)
