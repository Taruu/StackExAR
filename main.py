from app import app
from app.utils.config import settings
import uvicorn

if __name__ == "__main__":
    uvicorn.run(app, host=f"{settings.host}", port=settings.port)
