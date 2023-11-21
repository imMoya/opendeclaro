from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from fastapi.responses import HTMLResponse
from utils import process_csv

from opendeclaro import degiro

app = FastAPI()

# HTML form to upload a CSV file
html_form = """
<!DOCTYPE html>
<html>
<head>
    <title>CSV File Upload</title>
</head>
<body>
    <form action="/uploadfile/" enctype="multipart/form-data" method="post">
        <input type="file" name="file" accept=".csv">
        <input type="submit" value="Upload File">
    </form>
</body>
</html>
"""


@app.get("/", response_class=HTMLResponse)
async def get_upload_form():
    return HTMLResponse(content=html_form, status_code=200)


@app.post("/uploadfile/")
async def upload_file(file: UploadFile = File(...)):
    # Save the uploaded file
    with open(file.filename, "wb") as f:
        f.write(file.file.read())
    try:
        formatted_json = process_csv(file.filename)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="File not found")

    # Render the formatted JSON in the HTML response
    html_response_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Return of Portfolio</title>
    </head>
    <body>
        <h2>Summary of sales</h2>
        <pre>{formatted_json}</pre>
    </body>
    </html>
    """

    return HTMLResponse(content=html_response_content, status_code=200)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8000, log_level="info")
