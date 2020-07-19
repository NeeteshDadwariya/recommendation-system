from pydantic import BaseModel

class Request(BaseModel):
    user: int
    movie: int
