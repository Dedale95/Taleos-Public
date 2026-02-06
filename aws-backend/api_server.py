#!/usr/bin/env python3
"""
API FastAPI pour validation d'identifiants Crédit Agricole
Avec sécurité (API Key), rate limiting, CORS, monitoring
"""

import os
import logging
import time
from datetime import datetime
from typing import Optional
from collections import defaultdict

from fastapi import FastAPI, HTTPException, Depends, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, EmailStr, Field
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded

from selenium_validator import CreditAgricoleValidator

# Configuration
API_KEY = os.getenv('API_KEY', 'votre-cle-api-securisee-changez-moi')  # À configurer dans .env
ALLOWED_ORIGINS = os.getenv('ALLOWED_ORIGINS', '*').split(',')  # Frontend URLs
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')

# Logging structuré
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/var/log/validator_api.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Rate limiting
limiter = Limiter(key_func=get_remote_address)

# Application FastAPI
app = FastAPI(
    title="Crédit Agricole Validator API",
    description="API de validation d'identifiants bancaires",
    version="1.0.0",
    docs_url="/docs",  # Swagger UI
    redoc_url="/redoc"  # ReDoc
)

# CORS - À configurer selon votre domaine frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,  # ["https://votre-site.com"] en prod
    allow_credentials=True,
    allow_methods=["POST", "GET"],
    allow_headers=["*"],
)

# Rate limiting middleware
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# Metrics simples en mémoire (pour monitoring)
class Metrics:
    def __init__(self):
        self.total_requests = 0
        self.successful_validations = 0
        self.failed_validations = 0
        self.errors = 0
        self.total_execution_time = 0.0
        self.requests_by_hour = defaultdict(int)
    
    def record_request(self, success: bool, execution_time: float, error: bool = False):
        self.total_requests += 1
        hour = datetime.now().strftime('%Y-%m-%d %H:00')
        self.requests_by_hour[hour] += 1
        
        if error:
            self.errors += 1
        elif success:
            self.successful_validations += 1
        else:
            self.failed_validations += 1
        
        self.total_execution_time += execution_time
    
    def get_stats(self):
        avg_time = self.total_execution_time / self.total_requests if self.total_requests > 0 else 0
        return {
            'total_requests': self.total_requests,
            'successful_validations': self.successful_validations,
            'failed_validations': self.failed_validations,
            'errors': self.errors,
            'average_execution_time': round(avg_time, 2),
            'success_rate': round(self.successful_validations / self.total_requests * 100, 2) if self.total_requests > 0 else 0
        }

metrics = Metrics()

# Modèles Pydantic
class BankConnectionRequest(BaseModel):
    bank_id: str = Field(..., description="Identifiant de la banque (credit_agricole, etc.)")
    email: EmailStr = Field(..., description="Email de connexion")
    password: str = Field(..., min_length=1, description="Mot de passe")
    
    class Config:
        json_schema_extra = {
            "example": {
                "bank_id": "credit_agricole",
                "email": "user@example.com",
                "password": "motdepasse123"
            }
        }

class BankConnectionResponse(BaseModel):
    success: bool = Field(..., description="True si connexion réussie, False sinon")
    message: str = Field(..., description="Message descriptif")
    details: Optional[dict] = Field(None, description="Détails additionnels")


class ValidateRequest(BaseModel):
    """Format attendu par l'onglet Connexions (frontend) : bank_name, email, password."""
    bank_name: str = Field(..., description="Identifiant de la banque (credit_agricole, societe_generale, etc.)")
    email: str = Field(..., description="Email ou identifiant de connexion")
    password: str = Field(..., min_length=1, description="Mot de passe")


# Dépendance pour vérifier l'API Key
async def verify_api_key(request: Request):
    """Vérifie que l'API Key est présente et valide"""
    api_key = request.headers.get('X-API-Key')
    
    if not api_key:
        logger.warning(f"Requête sans API Key depuis {request.client.host}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="API Key manquante. Ajoutez le header 'X-API-Key'"
        )
    
    if api_key != API_KEY:
        logger.warning(f"API Key invalide depuis {request.client.host}")
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="API Key invalide"
        )
    
    return api_key

# Routes
@app.get("/")
async def root():
    """Page d'accueil de l'API"""
    return {
        "service": "Crédit Agricole Validator API",
        "status": "online",
        "version": "1.0.0",
        "docs": "/docs"
    }

@app.get("/health")
async def health_check():
    """Health check pour monitoring AWS"""
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat()
    }

@app.get("/metrics", dependencies=[Depends(verify_api_key)])
async def get_metrics():
    """Récupère les métriques de l'API (protégé par API Key)"""
    return metrics.get_stats()


async def _do_validate(bank_id: str, email: str, password: str) -> BankConnectionResponse:
    """Logique commune de validation (utilisée par /validate et /test-bank-connection)."""
    start_time = time.time()

    if bank_id != 'credit_agricole':
        logger.warning(f"Banque non supportée: {bank_id}")
        return BankConnectionResponse(
            success=False,
            message=f'Banque non supportée: {bank_id}. Seul "credit_agricole" est disponible.',
            details={'bank_id': bank_id}
        )

    try:
        validator = CreditAgricoleValidator(headless=True)
        result = validator.validate(email, password)
        execution_time = time.time() - start_time

        metrics.record_request(
            success=result.get('success', False),
            execution_time=execution_time
        )

        status_log = "SUCCÈS" if result.get('success', False) else "ÉCHEC"
        logger.info(f"{status_log} pour {email} ({bank_id}) - {execution_time:.2f}s")

        response_details = {
            'bank_id': bank_id,
            'execution_time': round(execution_time, 2)
        }
        if 'details' in result and isinstance(result.get('details'), dict) and 'url' in result['details']:
            response_details['url'] = result['details']['url']

        return BankConnectionResponse(
            success=result.get('success', False),
            message=result.get('message', 'Connexion échouée'),
            details=response_details
        )

    except Exception as e:
        execution_time = time.time() - start_time
        metrics.record_request(success=False, execution_time=execution_time, error=True)
        logger.error(f"Erreur lors de la validation pour {email} ({bank_id}): {e}", exc_info=True)
        return BankConnectionResponse(
            success=False,
            message=f'Erreur serveur: {str(e)}',
            details={
                'bank_id': bank_id,
                'error': str(e),
                'execution_time': round(execution_time, 2)
            }
        )


@app.post("/validate", response_model=BankConnectionResponse, dependencies=[Depends(verify_api_key)])
@limiter.limit("10/minute")
async def validate(request: Request, data: ValidateRequest):
    """
    Endpoint utilisé par l'onglet Connexions (frontend).
    Body: { "bank_name": "credit_agricole", "email": "...", "password": "..." }
    Réponse: { "success": true|false, "message": "...", "details": {...} }
    """
    logger.info(f"Validation /validate pour {data.bank_name} - {data.email} depuis {request.client.host}")
    return await _do_validate(data.bank_name, data.email, data.password)


@app.post("/test-bank-connection", response_model=BankConnectionResponse, dependencies=[Depends(verify_api_key)])
@limiter.limit("10/minute")
async def test_bank_connection(request: Request, data: BankConnectionRequest):
    """
    Teste la connexion à un site carrière bancaire.
    Body: { "bank_id": "credit_agricole", "email": "...", "password": "..." }
    Réponse: { "success": true|false, "message": "...", "details": {...} }
    """
    logger.info(f"Test connexion pour {data.bank_id} - {data.email} depuis {request.client.host}")
    return await _do_validate(data.bank_id, data.email, data.password)

# Gestion globale des erreurs
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logger.error(f"Erreur non gérée: {exc}", exc_info=True)
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={
            "status": "error",
            "message": "Une erreur interne s'est produite",
            "timestamp": datetime.now().isoformat()
        }
    )

# Point d'entrée pour développement
if __name__ == "__main__":
    import uvicorn
    
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000,
        reload=True,  # Auto-reload en dev
        log_level="info"
    )
