import os
import random
import shutil
import string

from app.api.utils import create_user_upload_folder, generate_random_str, process_csv
from fastapi import APIRouter, FastAPI, File, HTTPException, Request, UploadFile
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from opendeclaro import degiro

app = FastAPI(title="opendeclaro")

templates = Jinja2Templates(directory="app/api/templates")

UPLOAD_FOLDER = "app/api/uploads"
if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)


@app.get("/", response_class=HTMLResponse)
async def get_index() -> HTMLResponse:
    with open("app/api/templates/form.html") as f:
        html_content = f.read()
    return HTMLResponse(content=html_content, status_code=200)


@app.post("/uploadfile/", response_class=HTMLResponse)
async def create_upload_file(request: Request, file: UploadFile = File(...)):
    try:
        folder_path = os.path.join(UPLOAD_FOLDER, generate_random_str())
        create_user_upload_folder(folder_path)
        file_path = os.path.join(folder_path, file.filename)

        with open(file_path, "wb") as f:
            shutil.copyfileobj(file.file, f)

        formatted_json = process_csv(file_path)
        shutil.rmtree(folder_path)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="File not found")

    return templates.TemplateResponse("response.html", {"request": request, "formatted_json": formatted_json})


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8001, log_level="debug")
