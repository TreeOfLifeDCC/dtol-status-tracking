from pydantic import BaseModel, Field


class TokenSchema(BaseModel):
    access_token: str
    refresh_token: str


class TokenPayload(BaseModel):
    sub: str = None
    exp: int = None


class UserAuth(BaseModel):
    username: str = Field(..., description="user name")
    password: str = Field(..., min_length=5, max_length=24,
                          description="user password")


class UserOut(BaseModel):
    username: str


class SystemUser(UserOut):
    password: str
