from app.api.utils import process_csv
from fastapi import APIRouter, FastAPI, File, HTTPException, Request, UploadFile
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from opendeclaro import degiro

app = FastAPI(title="OpenDeclaro")

templates = Jinja2Templates(directory="app/api/templates")


@app.get("/", response_class=HTMLResponse)
async def get_index() -> HTMLResponse:
    with open("app/api/templates/form.html") as f:
        html_content = f.read()
    return HTMLResponse(content=html_content, status_code=200)


@app.post("/uploadfile/", response_class=HTMLResponse)
async def upload_file(request: Request, file: UploadFile = File(...)):
    try:
        formatted_json = process_csv(file.filename)  # Modify this line to obtain your formatted JSON
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="File not found")
    if not formatted_json:
        raise HTTPException(status_code=404, detail=f"Computation of P&L of stocks not successful")
    return templates.TemplateResponse("response.html", {"request": request, "formatted_json": formatted_json})


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8000, log_level="info")
