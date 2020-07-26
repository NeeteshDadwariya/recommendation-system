from fastapi import FastAPI
from src.entities.request import Request
app = FastAPI()

@app.post("/get-recommendations/")
def movie_ratings(request: Request):
    return ''
    #return recomm for the user+movie combination
