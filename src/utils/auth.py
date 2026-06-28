from datetime import datetime, timedelta, timezone
from jose import JWTError, jwt
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

from src.config import Config

config = Config()

# auto_error=False permite customizar a mensagem de erro
security = HTTPBearer(auto_error=False)


def create_access_token(data: dict) -> str:
    
    # Gera um token JWT com expiração configurável.
    to_encode = data.copy()
    expire = datetime.now(timezone.utc) + timedelta(minutes=config.JWT_EXPIRE_MINUTES)
    to_encode.update({"exp": expire})

    encoded_jwt = jwt.encode(
        to_encode,
        config.JWT_SECRET_KEY,
        algorithm=config.JWT_ALGORITHM
    )
    return encoded_jwt


def verify_token(token: str) -> dict:
    
    # Valida um token JWT e retorna o payload decodificado.
    try:
        payload = jwt.decode(
            token,
            config.JWT_SECRET_KEY,
            algorithms=[config.JWT_ALGORITHM]
        )
        return payload
    except JWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token inválido ou expirado",
            headers={"WWW-Authenticate": "Bearer"},
        )

def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security)
) -> dict:
    
    # Dependency para proteger rotas.
    # Só será usada quando adicionar Depends(get_current_user) no endpoint.
    if credentials is None or not credentials.credentials:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token não informado",
            headers={"WWW-Authenticate": "Bearer"},
        )

    token = credentials.credentials
    return verify_token(token)


def authenticate_user(username: str, password: str) -> bool:
    
    #Validação simples de usuário/senha com base no .env.
    return username == config.API_USERNAME and password == config.API_PASSWORD