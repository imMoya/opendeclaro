import json

from fastapi import FastAPI, File, Form, UploadFile
from fastapi.responses import HTMLResponse

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


def process_csv(data_path):
    try:
        deg_data = degiro.Dataset(data_path)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="File not found")

    # Perform some operation on the DataFrame (you can replace this with your own logic)
    deg_portfolio = degiro.Portfolio(deg_data.data)
    data = []
    for row in deg_portfolio.stock_sales.iter_rows(named=True):
        item = {"id_order": row["id_order"]}
        ret = deg_portfolio.return_of_sale(deg_data, row["product"], row["id_order"])
        item["name"] = row["product"]
        item["return"] = ret.return_value
        item["two_month_violation"] = ret.two_month_violation
        data.append(item)

    return json.dumps(data, indent=2)


@app.get("/", response_class=HTMLResponse)
async def get_upload_form():
    return HTMLResponse(content=html_form, status_code=200)


@app.post("/uploadfile/")
async def upload_file(file: UploadFile = File(...)):
    # Save the uploaded file
    with open(file.filename, "wb") as f:
        f.write(file.file.read())

    formatted_json = process_csv(file.filename)

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
