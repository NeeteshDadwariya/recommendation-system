from fastapi import FastAPI
from .entities.request import Request
app = FastAPI()

@app.post("/get-recommendations/")
def movie_ratings(request: Request):
    #return recomm for the user+movie combination
