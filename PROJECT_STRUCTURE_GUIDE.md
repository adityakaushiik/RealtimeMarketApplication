# Project Structure Guide: Realtime Market Application

## Table of Contents
1. [Overview](#overview)
2. [Option A: Feature-First (Vertical Modules)](#option-a-feature-first-vertical-modules)
3. [Option B: Layered (Horizontal Separation)](#option-b-layered-horizontal-separation)
4. [Comparison Matrix](#comparison-matrix)
5. [Dataflow Architecture](#dataflow-architecture)
6. [Migration from Current Structure](#migration-from-current-structure)
7. [Key Guidelines & Best Practices](#key-guidelines--best-practices)
8. [Code Examples](#code-examples)

---

## Overview

This document outlines two recommended architectural patterns for structuring a FastAPI-based realtime market data application with async SQLAlchemy, Redis pub/sub, WebSockets, and external data providers.

**Core Principles:**
- Clear separation of concerns (domain, infrastructure, API)
- Predictable dependency flow (inward: API → Service → Repository → Models)
- Async-first design for high-throughput realtime data
- Testability and maintainability

---

## Option A: Feature-First (Vertical Modules)

**Philosophy:** Group all code related to a single domain feature (auth, users, market_data) together. Each feature is self-contained with its own router, schemas, service, and repository. Shared infrastructure lives in `core/`.

### Directory Structure

```
RealtimeMarketApplication/
├── app/
│   ├── __init__.py
│   │
│   ├── core/                          # Shared infrastructure & cross-cutting concerns
│   │   ├── __init__.py
│   │   ├── settings.py                # Pydantic Settings (env vars, config)
│   │   ├── database.py                # AsyncEngine, get_db_session dependency
│   │   ├── redis.py                   # Redis client, pub/sub helpers, streams
│   │   ├── security.py                # Password hashing, JWT creation/verification
│   │   ├── logging.py                 # Structured logging config
│   │   └── constants.py               # App-wide constants (roles, statuses)
│   │
│   ├── models/                        # SQLAlchemy ORM models (shared)
│   │   ├── __init__.py
│   │   ├── base.py                    # Base declarative class
│   │   ├── user.py                    # User, Role models
│   │   ├── instrument.py              # Instrument, Exchange models
│   │   ├── price_history_daily.py
│   │   ├── price_history_intraday.py
│   │   ├── provider.py
│   │   └── mixins.py                  # Common model mixins (timestamps, etc.)
│   │
│   ├── events/                        # Shared event/message DTOs for pub/sub
│   │   ├── __init__.py
│   │   ├── market.py                  # TickEvent, BarEvent, SubscriptionEvent
│   │   └── system.py                  # HealthEvent, ProviderStatus
│   │
│   ├── features/                      # Feature modules (vertical slices)
│   │   │
│   │   ├── auth/                      # Authentication & authorization
│   │   │   ├── __init__.py
│   │   │   ├── router.py              # FastAPI endpoints (/auth/login, /auth/register)
│   │   │   ├── schemas.py             # LoginRequest, LoginResponse, TokenClaims
│   │   │   └── service.py             # authenticate_user, create_access_token wrapper
│   │   │
│   │   ├── users/                     # User management
│   │   │   ├── __init__.py
│   │   │   ├── router.py              # /users endpoints
│   │   │   ├── schemas.py             # UserCreate, UserUpdate, UserPublic, UserInDB
│   │   │   ├── repository.py          # get_by_email, get_by_username, create, exists
│   │   │   └── service.py             # register_user, assign_role, blacklist_user
│   │   │
│   │   ├── providers/                 # External data provider integrations
│   │   │   ├── __init__.py
│   │   │   ├── yahoo/
│   │   │   │   ├── __init__.py
│   │   │   │   ├── client.py          # Yahoo Finance API client
│   │   │   │   └── adapters.py        # Normalize Yahoo response → TickEvent/BarEvent
│   │   │   └── (future: alpha_vantage/, polygon/, etc.)
│   │   │
│   │   ├── market_data/               # Market data processing pipeline
│   │   │   ├── __init__.py
│   │   │   ├── ingestion/
│   │   │   │   ├── __init__.py
│   │   │   │   └── ingestor.py        # Pull from providers, normalize, emit to Redis
│   │   │   ├── aggregation/
│   │   │   │   ├── __init__.py
│   │   │   │   └── ohlcv.py           # Tick → OHLCV bar aggregation logic
│   │   │   └── persistence/
│   │   │       ├── __init__.py
│   │   │       └── writer.py          # Async batched DB writes, upsert strategy
│   │   │
│   │   └── realtime/                  # WebSocket & realtime broadcasting
│   │       ├── __init__.py
│   │       ├── router.py              # WebSocket endpoints
│   │       ├── websocket_manager.py   # Connection pool, broadcast logic
│   │       ├── subscriber.py          # Redis consumer (subscribe to events)
│   │       └── broadcast.py           # Format & send messages to WebSocket clients
│   │
│   └── api/                           # API composition layer
│       ├── __init__.py
│       └── routers.py                 # Aggregate & register all feature routers
│
├── migrations/                        # Alembic migration versions
│   └── versions/
│
├── tests/                             # Test suite (mirrors app structure)
│   ├── __init__.py
│   ├── conftest.py                    # Shared fixtures (test DB, client, etc.)
│   ├── core/
│   ├── features/
│   │   ├── auth/
│   │   ├── users/
│   │   ├── market_data/
│   │   └── realtime/
│   └── integration/
│
├── alembic/                           # Alembic configuration
│   ├── env.py
│   ├── README
│   └── script.py.mako
│
├── main.py                            # FastAPI app creation, lifespan, startup/shutdown
├── requirements.txt
├── Dockerfile
├── docker-compose.yml
└── README.md
```

### Feature Module Structure (Detailed)

Each feature follows the same internal pattern:

```
features/<feature_name>/
├── __init__.py
├── router.py          # FastAPI APIRouter with HTTP/WebSocket endpoints
├── schemas.py         # Pydantic models for request/response validation
├── service.py         # Business logic, orchestration, calls repository
├── repository.py      # (optional) Data access layer, query construction
├── exceptions.py      # (optional) Feature-specific exceptions
└── constants.py       # (optional) Feature-specific constants
```

**Dependency Flow:**
```
router.py
  ↓ (calls)
service.py
  ↓ (calls)
repository.py
  ↓ (queries)
models/ (ORM)
```

Schemas are used by both router (input/output) and service (DTOs).

---

## Option B: Layered (Horizontal Separation)

**Philosophy:** Organize by technical layer (API, services, repositories, models). All routers together, all services together, etc. Better for projects with many shared components or when following strict n-tier architecture.

### Directory Structure

```
RealtimeMarketApplication/
├── app/
│   ├── __init__.py
│   │
│   ├── core/                          # Same as Option A
│   │   ├── __init__.py
│   │   ├── settings.py
│   │   ├── database.py
│   │   ├── redis.py
│   │   ├── security.py
│   │   ├── logging.py
│   │   └── constants.py
│   │
│   ├── models/                        # All SQLAlchemy ORM models
│   │   ├── __init__.py
│   │   ├── base.py
│   │   ├── user.py
│   │   ├── role.py
│   │   ├── instrument.py
│   │   ├── price_history_daily.py
│   │   ├── price_history_intraday.py
│   │   ├── provider.py
│   │   └── mixins.py
│   │
│   ├── schemas/                       # All Pydantic schemas
│   │   ├── __init__.py
│   │   ├── auth.py                    # LoginRequest, LoginResponse, TokenClaims
│   │   ├── user.py                    # UserCreate, UserUpdate, UserPublic, UserInDB
│   │   ├── instrument.py              # InstrumentPublic, InstrumentCreate
│   │   ├── market.py                  # TickEvent, BarEvent, SubscriptionEvent
│   │   └── system.py                  # HealthEvent, ProviderStatus
│   │
│   ├── repositories/                  # Data access layer
│   │   ├── __init__.py
│   │   ├── base.py                    # BaseRepository with common CRUD
│   │   ├── user_repository.py
│   │   ├── instrument_repository.py
│   │   └── price_history_repository.py
│   │
│   ├── services/                      # Business logic layer
│   │   ├── __init__.py
│   │   ├── auth_service.py            # authenticate_user, token operations
│   │   ├── user_service.py            # register_user, assign_role, blacklist
│   │   ├── ingestion_service.py       # Pull from providers, normalize
│   │   ├── aggregation_service.py     # Tick → bar aggregation
│   │   ├── persistence_service.py     # Async DB writes
│   │   └── broadcast_service.py       # WebSocket broadcast logic
│   │
│   ├── api/                           # API layer
│   │   ├── __init__.py
│   │   ├── dependencies.py            # Shared API dependencies
│   │   └── routers/
│   │       ├── __init__.py
│   │       ├── auth.py                # /auth endpoints
│   │       ├── users.py               # /users endpoints
│   │       ├── instruments.py         # /instruments endpoints
│   │       └── realtime.py            # WebSocket endpoints
│   │
│   ├── providers/                     # External integrations (can be feature-ish)
│   │   ├── __init__.py
│   │   ├── yahoo/
│   │   │   ├── __init__.py
│   │   │   ├── client.py
│   │   │   └── adapters.py
│   │   └── (future providers)
│   │
│   ├── workers/                       # Background tasks & consumers
│   │   ├── __init__.py
│   │   ├── redis_subscriber.py        # Redis event consumer
│   │   ├── market_data_worker.py      # Scheduled ingestion tasks
│   │   └── persistence_worker.py      # Batch DB writer
│   │
│   └── utils/                         # Utility functions
│       ├── __init__.py
│       ├── ohlcv_to_binary.py
│       └── helpers.py
│
├── migrations/
│   └── versions/
│
├── tests/
│   ├── __init__.py
│   ├── conftest.py
│   ├── api/
│   ├── services/
│   ├── repositories/
│   └── integration/
│
├── alembic/
├── main.py
├── requirements.txt
├── Dockerfile
├── docker-compose.yml
└── README.md
```

### Layered Dependency Flow

```
API Layer (routers/)
    ↓
Services Layer (services/)
    ↓
Repositories Layer (repositories/)
    ↓
Models Layer (models/)
```

All schemas defined centrally in `schemas/`.

---

## Comparison Matrix

| Aspect | Feature-First (Option A) | Layered (Option B) |
|--------|--------------------------|-------------------|
| **Organization** | By domain/feature | By technical layer |
| **Cohesion** | High (related code together) | Lower (code scattered) |
| **Navigation** | Easier for feature work | Easier for layer-specific refactors |
| **Scalability** | Excellent for many features | Better for very deep layers |
| **Code Reuse** | Share via `core/` and `models/` | Natural layer-wide sharing |
| **Testing** | Feature-scoped tests | Layer-scoped tests |
| **Onboarding** | Faster (clear feature boundaries) | Slower (must understand all layers) |
| **Refactoring** | Extract features easily | Extract layers easily |
| **Best For** | Microservice-style monoliths, domain-driven | Traditional n-tier, shared infrastructure |

**Recommendation for Realtime Market App:** **Option A (Feature-First)** is preferred because:
- Clear domain boundaries (auth, market_data, realtime)
- Easier to scale team (assign features to developers)
- Natural fit for eventual microservice extraction
- Realtime features are self-contained with their own pub/sub logic

---

## Dataflow Architecture

### End-to-End Realtime Pipeline

```
┌─────────────────┐
│  External APIs  │ (Yahoo Finance, etc.)
└────────┬────────┘
         │ HTTP polling / WebSocket
         ↓
┌─────────────────────────┐
│  Provider Client        │ (features/providers/yahoo/client.py)
│  (Fetch raw data)       │
└────────┬────────────────┘
         │
         ↓
┌─────────────────────────┐
│  Adapter / Normalizer   │ (features/providers/yahoo/adapters.py)
│  (Raw → TickEvent)      │
└────────┬────────────────┘
         │
         ↓
┌─────────────────────────┐
│  Ingestor               │ (features/market_data/ingestion/ingestor.py)
│  (Validate, enrich)     │
└────────┬────────────────┘
         │
         ├──────────────────┐
         │                  │
         ↓                  ↓
┌────────────────┐   ┌──────────────────┐
│ Redis Pub/Sub  │   │  Aggregator      │ (features/market_data/aggregation/ohlcv.py)
│ or Streams     │   │  (Tick → Bar)    │
└───────┬────────┘   └────────┬─────────┘
        │                     │
        │                     ↓
        │            ┌─────────────────┐
        │            │  Redis (Bars)   │
        │            └────────┬────────┘
        │                     │
        ├─────────────────────┘
        │
        ↓
┌──────────────────────┐
│  Redis Subscriber    │ (features/realtime/subscriber.py)
│  (Consume events)    │
└──────────┬───────────┘
           │
           ↓
┌──────────────────────┐
│  Broadcast Service   │ (features/realtime/broadcast.py)
│  (Format messages)   │
└──────────┬───────────┘
           │
           ↓
┌──────────────────────┐
│  WebSocket Manager   │ (features/realtime/websocket_manager.py)
│  (Send to clients)   │
└──────────────────────┘

        ┌──────────────────────┐
        │  Persistence Writer  │ (features/market_data/persistence/writer.py)
        │  (Async batch upsert)│
        └──────────┬───────────┘
                   │
                   ↓
        ┌──────────────────────┐
        │    PostgreSQL DB     │
        └──────────────────────┘
```

### Key Components

1. **Provider Clients**: Fetch raw data from external APIs
2. **Adapters**: Normalize provider-specific formats → standard events
3. **Ingestor**: Validate, enrich, emit to Redis
4. **Aggregator**: (Optional) Tick → OHLCV bar conversion
5. **Redis**: Message broker (Pub/Sub for ephemeral, Streams for durable)
6. **Subscriber**: Consume events from Redis
7. **Broadcast**: Format & send to WebSocket clients
8. **Persistence**: Async batched writes to DB

---

## Migration from Current Structure

### Current → Target Mapping (Feature-First)

| Current Path | Target Path (Option A) |
|--------------|------------------------|
| `config/settings.py` | `app/core/settings.py` |
| `config/database_config.py` | `app/core/database.py` |
| `config/redis_config.py` | `app/core/redis.py` |
| `config/logger.py` | `app/core/logging.py` |
| `models/*.py` | `app/models/*.py` |
| `modules/auth/auth_routes.py` | `app/features/auth/router.py` |
| `modules/auth/auth_service.py` | `app/features/auth/service.py` |
| `modules/auth/auth_validation.py` | `app/features/auth/schemas.py` |
| `modules/user/user_service.py` | `app/features/users/service.py` |
| `modules/user/user_route.py` | `app/features/users/router.py` |
| `modules/user/user_validation.py` | `app/features/users/schemas.py` |
| `routes/web_socket_routes.py` | `app/features/realtime/router.py` |
| `services/websocket_manager.py` | `app/features/realtime/websocket_manager.py` |
| `services/data_broadcast.py` | `app/features/realtime/broadcast.py` |
| `services/redis_subscriber.py` | `app/features/realtime/subscriber.py` |
| `services/live_data_ingestion.py` | `app/features/market_data/ingestion/ingestor.py` |
| `services/yahoo_finance_connection.py` | `app/features/providers/yahoo/client.py` |
| `services/redis_helper.py` | `app/core/redis.py` |
| `services/in_memory_db.py` | `app/core/cache.py` (or in-memory helper) |
| `utils/ohlcv_to_binary.py` | `app/features/market_data/aggregation/ohlcv.py` |
| `utils/common_constants.py` | `app/core/constants.py` |
| `alembic/versions/` | `migrations/versions/` (or keep alembic/) |

### Incremental Migration Steps

#### Phase 1: Core Infrastructure (No Breaking Changes)
1. Create `app/core/` directory
2. Copy `config/` files → `app/core/` (keep originals)
3. Update `app/core/database.py` to use new imports
4. Test: Ensure `get_db_session` works from new location
5. Update `main.py` to import from `app.core.database`

#### Phase 2: Models (Safe Move)
1. Create `app/models/` directory
2. Move all files from `models/` → `app/models/`
3. Update `__init__.py` to export all models
4. Global find-replace: `from models` → `from app.models`
5. Test: Run Alembic check, ensure migrations work

#### Phase 3: Auth Feature
1. Create `app/features/auth/`
2. Move & rename:
   - `auth_routes.py` → `router.py`
   - `auth_validation.py` → `schemas.py`
   - `auth_service.py` → `service.py`
3. Extract password/JWT functions from `service.py` → `app/core/security.py`
4. Update imports in `router.py` and `service.py`
5. Update `main.py`: `app.include_router(auth_router)` → `from app.features.auth.router import router as auth_router`
6. Test: `/auth/login` and `/auth/register` endpoints

#### Phase 4: Users Feature
1. Create `app/features/users/`
2. Move `user_service.py` → `service.py`
3. Move `user_validation.py` → `schemas.py`
4. Create `repository.py` with query methods
5. Update `service.py` to use `repository.py`
6. Fix circular import: `UserInDB` from schemas, not from auth router
7. Test: User creation, queries

#### Phase 5: Realtime Feature
1. Create `app/features/realtime/`
2. Move:
   - `web_socket_routes.py` → `router.py`
   - `websocket_manager.py` → `websocket_manager.py`
   - `data_broadcast.py` → `broadcast.py`
   - `redis_subscriber.py` → `subscriber.py`
3. Update imports
4. Test: WebSocket connections, message broadcast

#### Phase 6: Market Data & Providers
1. Create `app/features/providers/yahoo/`
2. Move `yahoo_finance_connection.py` → `client.py`
3. Create `adapters.py` for normalization
4. Create `app/events/market.py` with event schemas
5. Create `app/features/market_data/ingestion/ingestor.py`
6. Refactor ingestion to emit events
7. Test: Data ingestion pipeline

#### Phase 7: Cleanup
1. Remove old `config/`, `modules/`, `services/`, `routes/` directories
2. Update all absolute imports
3. Run full test suite
4. Update documentation

---

## Key Guidelines & Best Practices

### 1. Async SQLAlchemy Patterns

#### Session Management
```python
# Good: Transaction at route level
@router.post("/users")
async def create_user(payload: UserCreate, session: AsyncSession = Depends(get_db_session)):
    async with session.begin():  # Auto-commit on success, rollback on exception
        user = await user_service.register_user(session, payload)
    return UserPublic.model_validate(user)
```

#### Query Patterns
```python
# Single object
stmt = select(User).where(User.email == email)
result = await session.execute(stmt)
user = result.scalar_one_or_none()  # None or raises if multiple

# Multiple objects
stmt = select(User).where(User.blacklisted.is_(False)).limit(10)
result = await session.execute(stmt)
users = result.scalars().all()  # List of User objects

# Primary key lookup (fastest)
user = await session.get(User, user_id)

# Existence check
stmt = select(exists().where(User.email == email))
exists_flag = (await session.execute(stmt)).scalar()

# With relationships (eager loading)
from sqlalchemy.orm import joinedload
stmt = select(User).options(joinedload(User.role)).where(User.id == user_id)
user = (await session.execute(stmt)).scalar_one_or_none()
```

### 2. Service Layer Rules

**Services should:**
- Return domain objects (ORM models)
- Orchestrate business logic
- Call repositories for data access
- Raise domain exceptions (not HTTP exceptions)
- Be framework-agnostic (no FastAPI imports)

**Services should NOT:**
- Import routers
- Commit transactions (let caller control)
- Return Pydantic schemas (router's job)

```python
# Good
async def register_user(session: AsyncSession, data: UserCreate) -> User:
    if await user_repo.exists_email(session, data.email):
        raise DuplicateEmailError(f"Email {data.email} already exists")
    hashed_password = hash_password(data.password)
    user = await user_repo.create(session, data, hashed_password)
    return user  # Return ORM object

# Bad
async def register_user(session: AsyncSession, data: UserCreate) -> UserPublic:
    # ... logic ...
    await session.commit()  # Don't commit here!
    return UserPublic.model_validate(user)  # Don't return Pydantic here!
```

### 3. Repository Layer (Optional but Recommended)

```python
class UserRepository:
    async def get_by_id(self, session: AsyncSession, user_id: int) -> User | None:
        return await session.get(User, user_id)

    async def get_by_email(self, session: AsyncSession, email: str) -> User | None:
        stmt = select(User).where(User.email == email)
        return (await session.execute(stmt)).scalar_one_or_none()

    async def exists_email(self, session: AsyncSession, email: str) -> bool:
        stmt = select(exists().where(User.email == email))
        return (await session.execute(stmt)).scalar()

    async def create(self, session: AsyncSession, data: UserCreate, hashed_password: str) -> User:
        user = User(
            email=data.email,
            hashed_password=hashed_password,
            fname=data.fname,
            lname=data.lname,
            username=data.username,
            profile_picture_url=data.profile_picture_url,
        )
        session.add(user)
        await session.flush()  # Get PK without committing
        return user

    async def list_active(self, session: AsyncSession, limit: int = 100) -> list[User]:
        stmt = select(User).where(User.blacklisted.is_(False)).limit(limit)
        return (await session.execute(stmt)).scalars().all()
```

### 4. Event Schema Design

```python
# app/events/market.py
from datetime import datetime
from pydantic import BaseModel, Field

class TickEvent(BaseModel):
    """Real-time tick/quote event."""
    symbol: str
    timestamp: datetime
    bid: float | None = None
    ask: float | None = None
    last: float | None = None
    volume: int | None = None
    provider: str  # "yahoo", "polygon", etc.
    sequence: int | None = None  # For ordering/dedup

class BarEvent(BaseModel):
    """OHLCV bar event."""
    symbol: str
    interval: str  # "1s", "1m", "5m", "1h", "1d"
    timestamp: datetime  # Bar start time
    open: float
    high: float
    low: float
    close: float
    volume: int
    source: str = "aggregated"  # "aggregated" or "provider"

class SubscriptionEvent(BaseModel):
    """Client subscription request."""
    action: str  # "subscribe" or "unsubscribe"
    symbols: list[str]
    interval: str | None = "1s"
    client_id: str
```

### 5. Redis Patterns

#### Pub/Sub (Ephemeral, No Persistence)
```python
# Publisher (in ingestor)
import json
await redis.publish("market:ticks", tick_event.model_dump_json())

# Subscriber (in realtime/subscriber.py)
pubsub = redis.pubsub()
await pubsub.subscribe("market:ticks")
async for message in pubsub.listen():
    if message["type"] == "message":
        tick = TickEvent.model_validate_json(message["data"])
        await broadcast_service.send_tick(tick)
```

#### Streams (Durable, Replay, Consumer Groups)
```python
# Publisher
await redis.xadd("market:ticks", {"data": tick_event.model_dump_json()})

# Consumer
while True:
    messages = await redis.xread({"market:ticks": last_id}, count=100, block=1000)
    for stream, msg_list in messages:
        for msg_id, fields in msg_list:
            tick = TickEvent.model_validate_json(fields["data"])
            await process_tick(tick)
            last_id = msg_id
```

### 6. WebSocket Manager Pattern

```python
# app/features/realtime/websocket_manager.py
from fastapi import WebSocket
from collections import defaultdict

class ConnectionManager:
    def __init__(self):
        self.active_connections: dict[str, list[WebSocket]] = defaultdict(list)
        # symbol -> [websocket1, websocket2, ...]

    async def connect(self, websocket: WebSocket, symbol: str):
        await websocket.accept()
        self.active_connections[symbol].append(websocket)

    def disconnect(self, websocket: WebSocket, symbol: str):
        self.active_connections[symbol].remove(websocket)

    async def broadcast_to_symbol(self, symbol: str, message: dict):
        dead_connections = []
        for connection in self.active_connections[symbol]:
            try:
                await connection.send_json(message)
            except:
                dead_connections.append(connection)
        for conn in dead_connections:
            self.disconnect(conn, symbol)

manager = ConnectionManager()
```

### 7. Idempotency & Deduplication

```python
# Upsert pattern for bars (PostgreSQL)
from sqlalchemy.dialects.postgresql import insert

stmt = insert(PriceHistoryIntraday).values(
    symbol=bar.symbol,
    interval=bar.interval,
    timestamp=bar.timestamp,
    open=bar.open,
    high=bar.high,
    low=bar.low,
    close=bar.close,
    volume=bar.volume,
)
stmt = stmt.on_conflict_do_update(
    index_elements=["symbol", "interval", "timestamp"],
    set_={
        "open": stmt.excluded.open,
        "high": stmt.excluded.high,
        "low": stmt.excluded.low,
        "close": stmt.excluded.close,
        "volume": stmt.excluded.volume,
        "updated_at": func.now(),
    }
)
await session.execute(stmt)
```

### 8. Testing Strategy

```python
# tests/conftest.py
import pytest
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker

@pytest.fixture
async def test_db():
    engine = create_async_engine("postgresql+asyncpg://test:test@localhost/test_db")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    
    async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    
    async with async_session() as session:
        yield session
    
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
    await engine.dispose()

# tests/features/users/test_service.py
async def test_register_user_success(test_db):
    data = UserCreate(email="test@example.com", password="secure123")
    user = await user_service.register_user(test_db, data)
    assert user.email == "test@example.com"
    assert user.id is not None

async def test_register_duplicate_email_raises(test_db):
    data = UserCreate(email="test@example.com", password="secure123")
    await user_service.register_user(test_db, data)
    
    with pytest.raises(DuplicateEmailError):
        await user_service.register_user(test_db, data)
```

### 9. Background Workers

```python
# app/workers/market_data_worker.py
import asyncio
from app.core.database import get_database_engine
from app.core.redis import get_redis_client

async def market_data_worker():
    """Background task: ingest data from providers."""
    engine = get_database_engine()
    redis = get_redis_client()
    
    while True:
        try:
            # Pull data from provider
            ticks = await yahoo_client.fetch_realtime_quotes(symbols)
            
            # Normalize & publish
            for tick in ticks:
                event = TickEvent(...)
                await redis.publish("market:ticks", event.model_dump_json())
            
            await asyncio.sleep(1)  # Poll interval
        except Exception as e:
            logger.error(f"Worker error: {e}")
            await asyncio.sleep(5)

# main.py
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    task = asyncio.create_task(market_data_worker())
    yield
    # Shutdown
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

app = FastAPI(lifespan=lifespan)
```

### 10. Error Handling

```python
# app/features/users/exceptions.py
class UserError(Exception):
    """Base user exception."""

class DuplicateEmailError(UserError):
    """Email already registered."""

class DuplicateUsernameError(UserError):
    """Username already taken."""

# app/features/users/router.py
@router.post("/")
async def create_user(payload: UserCreate, session: AsyncSession = Depends(get_db_session)):
    async with session.begin():
        try:
            user = await user_service.register_user(session, payload)
        except DuplicateEmailError:
            raise HTTPException(status_code=400, detail="Email already registered")
        except DuplicateUsernameError:
            raise HTTPException(status_code=400, detail="Username already taken")
    return UserPublic.model_validate(user)
```

---

## Code Examples

### Example 1: Complete Auth Feature (Feature-First)

#### `app/features/auth/schemas.py`
```python
from pydantic import BaseModel, EmailStr

class LoginRequest(BaseModel):
    username_or_email: str
    password: str

class LoginResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"

class TokenClaims(BaseModel):
    id: str
    email: EmailStr
    roles: list[str]
```

#### `app/features/auth/service.py`
```python
from sqlalchemy import select, or_
from sqlalchemy.ext.asyncio import AsyncSession
from app.models.user import User
from app.core.security import verify_password

async def authenticate_user(
    session: AsyncSession,
    username_or_email: str,
    password: str
) -> User | None:
    """Authenticate user by username/email and password."""
    stmt = select(User).where(
        or_(
            User.email == username_or_email,
            User.username == username_or_email
        )
    )
    result = await session.execute(stmt)
    user = result.scalar_one_or_none()
    
    if not user or user.blacklisted:
        return None
    
    if not verify_password(password, user.hashed_password):
        return None
    
    return user
```

#### `app/features/auth/router.py`
```python
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from app.core.database import get_db_session
from app.core.security import create_access_token
from .schemas import LoginRequest, LoginResponse
from .service import authenticate_user

router = APIRouter(prefix="/auth", tags=["auth"])

@router.post("/login", response_model=LoginResponse)
async def login(
    payload: LoginRequest,
    session: AsyncSession = Depends(get_db_session)
):
    user = await authenticate_user(session, payload.username_or_email, payload.password)
    if not user:
        raise HTTPException(status_code=401, detail="Invalid credentials")
    
    claims = {
        "id": str(user.id),
        "email": user.email,
        "roles": [user.role_id] if user.role_id else [],
    }
    token = create_access_token(claims)
    
    return LoginResponse(access_token=token)
```

### Example 2: Complete Users Feature

#### `app/features/users/repository.py`
```python
from sqlalchemy import select, exists
from sqlalchemy.ext.asyncio import AsyncSession
from app.models.user import User
from .schemas import UserCreate

class UserRepository:
    async def get_by_email(self, session: AsyncSession, email: str) -> User | None:
        stmt = select(User).where(User.email == email)
        return (await session.execute(stmt)).scalar_one_or_none()

    async def get_by_username(self, session: AsyncSession, username: str) -> User | None:
        stmt = select(User).where(User.username == username)
        return (await session.execute(stmt)).scalar_one_or_none()

    async def exists_email(self, session: AsyncSession, email: str) -> bool:
        stmt = select(exists().where(User.email == email))
        return (await session.execute(stmt)).scalar()

    async def create(self, session: AsyncSession, data: UserCreate, hashed_password: str) -> User:
        user = User(
            email=data.email,
            hashed_password=hashed_password,
            fname=data.fname,
            lname=data.lname,
            username=data.username,
            profile_picture_url=data.profile_picture_url,
        )
        session.add(user)
        await session.flush()
        return user

user_repository = UserRepository()
```

#### `app/features/users/service.py`
```python
from sqlalchemy.ext.asyncio import AsyncSession
from app.core.security import hash_password
from app.models.user import User
from .repository import user_repository
from .schemas import UserCreate

class DuplicateEmailError(Exception):
    pass

class DuplicateUsernameError(Exception):
    pass

async def register_user(session: AsyncSession, data: UserCreate) -> User:
    """Register a new user."""
    if await user_repository.exists_email(session, data.email):
        raise DuplicateEmailError(f"Email {data.email} already exists")
    
    if data.username and await user_repository.get_by_username(session, data.username):
        raise DuplicateUsernameError(f"Username {data.username} already taken")
    
    hashed_password = hash_password(data.password)
    user = await user_repository.create(session, data, hashed_password)
    return user
```

#### `app/features/users/router.py`
```python
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from app.core.database import get_db_session
from .schemas import UserCreate, UserPublic
from .service import register_user, DuplicateEmailError, DuplicateUsernameError

router = APIRouter(prefix="/users", tags=["users"])

@router.post("/", response_model=UserPublic, status_code=201)
async def create_user(
    payload: UserCreate,
    session: AsyncSession = Depends(get_db_session)
):
    async with session.begin():
        try:
            user = await register_user(session, payload)
        except DuplicateEmailError as e:
            raise HTTPException(status_code=400, detail=str(e))
        except DuplicateUsernameError as e:
            raise HTTPException(status_code=400, detail=str(e))
    
    return UserPublic.model_validate(user)
```

### Example 3: Market Data Ingestion

#### `app/features/providers/yahoo/client.py`
```python
import aiohttp
from typing import Any

class YahooFinanceClient:
    def __init__(self, base_url: str = "https://query1.finance.yahoo.com"):
        self.base_url = base_url
    
    async def fetch_quote(self, symbol: str) -> dict[str, Any]:
        """Fetch real-time quote for a symbol."""
        async with aiohttp.ClientSession() as session:
            url = f"{self.base_url}/v7/finance/quote"
            params = {"symbols": symbol}
            async with session.get(url, params=params) as response:
                data = await response.json()
                return data["quoteResponse"]["result"][0]

yahoo_client = YahooFinanceClient()
```

#### `app/features/providers/yahoo/adapters.py`
```python
from datetime import datetime
from app.events.market import TickEvent

def yahoo_quote_to_tick(raw: dict, symbol: str) -> TickEvent:
    """Convert Yahoo quote to TickEvent."""
    return TickEvent(
        symbol=symbol,
        timestamp=datetime.fromtimestamp(raw.get("regularMarketTime", 0)),
        bid=raw.get("bid"),
        ask=raw.get("ask"),
        last=raw.get("regularMarketPrice"),
        volume=raw.get("regularMarketVolume"),
        provider="yahoo",
        sequence=None,
    )
```

#### `app/features/market_data/ingestion/ingestor.py`
```python
import asyncio
from app.core.redis import get_redis_client
from app.features.providers.yahoo.client import yahoo_client
from app.features.providers.yahoo.adapters import yahoo_quote_to_tick

async def ingest_symbol(symbol: str):
    """Fetch and publish tick for a symbol."""
    redis = get_redis_client()
    
    while True:
        try:
            raw_quote = await yahoo_client.fetch_quote(symbol)
            tick = yahoo_quote_to_tick(raw_quote, symbol)
            await redis.publish("market:ticks", tick.model_dump_json())
        except Exception as e:
            print(f"Ingestion error for {symbol}: {e}")
        
        await asyncio.sleep(1)

async def start_ingestion(symbols: list[str]):
    """Start ingestion for multiple symbols."""
    tasks = [asyncio.create_task(ingest_symbol(sym)) for sym in symbols]
    await asyncio.gather(*tasks)
```

---

## Summary

### When to Use Feature-First (Option A)
- ✅ You're building a domain-rich application
- ✅ Multiple developers/teams working on different features
- ✅ Planning to extract microservices later
- ✅ Clear feature boundaries (auth, market_data, realtime)
- ✅ Rapid iteration on individual features

### When to Use Layered (Option B)
- ✅ Small team, shared codebase mindset
- ✅ Heavy code reuse across features
- ✅ Traditional n-tier architecture preference
- ✅ Strong layer-specific testing requirements
- ✅ Many shared repositories/services

### Hybrid Approach
You can mix both:
- Use **feature modules** for domain logic (auth, users, market_data, realtime)
- Use **layered structure** for shared infrastructure (core, models, schemas)
- Keep providers separate (they're integrations, not core domain)

**For a realtime market application, Feature-First (Option A) is recommended** due to clear domain separation, scalability, and easier team coordination.

---

## Next Steps

1. Choose your preferred structure (A or B)
2. Follow the incremental migration plan
3. Start with Phase 1 (Core) - no breaking changes
4. Gradually migrate features one by one
5. Add tests as you refactor
6. Update documentation and team guidelines

Good luck with your refactor! 🚀

