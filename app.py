from fastapi import FastAPI, UploadFile, File
import json
from Main import Execute_LTROI

app = FastAPI()

@app.post("/run/")
async def run_pipeline(config_file: UploadFile = File(...)):
    config = json.loads(await config_file.read())
    result = Execute_LTROI(config)
    return result
