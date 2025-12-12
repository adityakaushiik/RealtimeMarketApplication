# Realtime Market Application

A comprehensive real-time market data application built with FastAPI, designed to ingest, process, and serve live financial market data from multiple providers. The application supports real-time WebSocket subscriptions, historical data storage, and scalable data ingestion pipelines.

## Table of Contents

- [Features](#features)
- [Architecture](#architecture)
- [Database Models](#database-models)
- [Technologies Used](#technologies-used)
- [Prerequisites](#prerequisites)
- [API Documentation](#api-documentation)
- [Data Ingestion](#data-ingestion)
- [WebSocket Usage](#websocket-usage)
- [Development](#development)
- [Contributing](#contributing)

## Features

### Core Functionality
- **Real-time Data Ingestion**: Continuous streaming of market data from multiple financial data providers
- **WebSocket Subscriptions**: Real-time price updates via WebSocket connections
- **Historical Data Storage**: Intraday and daily price history with OHLCV (Open, High, Low, Close, Volume) data
- **Multi-Provider Support**: Integration with Yahoo Finance and Dhan APIs
- **Exchange Management**: CRUD operations for stock exchanges with timezone and market hours support
- **Instrument Management**: Comprehensive instrument database with symbol mapping across providers
- **User Authentication**: JWT-based authentication with role-based access control
- **Redis Time Series**: High-performance time series data storage and retrieval

### API Endpoints
- **Exchange Management**: Create, read, update, delete exchanges
- **Instrument Management**: Instrument CRUD with provider mappings
- **Market Data**: Historical price data retrieval (intraday and daily)
- **Authentication**: User registration, login, and token management
- **Database Population**: Automated data seeding for instruments and exchanges

### Background Services
- **Live Data Ingestion**: Asynchronous data processing from multiple providers
- **Data Broadcasting**: Real-time WebSocket broadcasting of price updates
- **Data Persistence**: Scheduled saving of intraday data to database
- **Redis Caching**: Time series caching for fast data access

## Architecture

The application follows a modular architecture with clear separation of concerns:

```
├── features/           # API feature modules (auth, exchange, instruments, etc.)
├── models/            # SQLAlchemy database models
├── services/          # Business logic and background services
├── config/            # Configuration management
├── utils/             # Utility functions and constants
├── z_data_migration/  # Database setup and migration scripts
└── alembic/           # Database migration management
```

### Data Flow
1. **Data Ingestion**: Providers stream data → ProviderManager routes to appropriate handlers
2. **Processing**: Data normalized and queued for batch processing
3. **Storage**: Data saved to Redis Time Series for real-time access and PostgreSQL for persistence
4. **Broadcasting**: WebSocket manager broadcasts updates to subscribed clients
5. **Caching**: Redis provides fast access to recent data

## Database Models

The application uses SQLAlchemy ORM with async support for database operations. Key models include:

### Core Entities
- **User**: Manages user accounts with roles (admin, user). Supports JWT authentication and Google OAuth.
- **Exchange**: Represents stock exchanges with timezone, market hours, and currency information. Supports 24-hour markets.
- **Instrument**: Financial instruments (stocks, etc.) linked to exchanges, with symbol mapping across providers.
- **Provider**: Data providers (Yahoo Finance, Dhan) with rate limits and credentials.
- **Sector**: Industry sectors for instrument categorization.

### Data Storage
- **PriceHistoryIntraday**: Stores OHLCV data for intraday trading with timestamps, volumes, and delivery percentages.
- **PriceHistoryDaily**: Daily price summaries with adjusted close prices.

### Mappings and Relationships
- **ExchangeProviderMapping**: Links exchanges to providers for data sourcing.
- **ProviderInstrumentMapping**: Maps instrument symbols to provider-specific search codes for accurate data retrieval.

All models inherit from a base mixin providing created_at, updated_at, and is_active fields for audit trails.

## Technologies Used

### Backend Framework
- **Python 3.13**: Core programming language
- **FastAPI**: Modern, fast web framework for building APIs
- **Uvicorn**: ASGI server for FastAPI
- **Starlette**: ASGI toolkit for FastAPI

### Database & ORM
- **PostgreSQL**: Primary relational database
- **SQLAlchemy 2.0**: Async ORM for database operations
- **Alembic**: Database migration tool
- **AsyncPG**: PostgreSQL driver for async operations

### Caching & Time Series
- **Redis Stack**: In-memory data structure store with Time Series, JSON, and Search modules
- **RedisTimeSeries**: Specialized time series data handling

### Authentication & Security
- **PyJWT**: JSON Web Token implementation
- **PassLib**: Password hashing utilities
- **Pydantic**: Data validation and settings management

### Data Providers
- **yfinance**: Yahoo Finance API client
- **dhanhq**: Dhan API client for Indian markets

### WebSocket & Real-time
- **WebSockets**: Real-time bidirectional communication
- **WebSocketManager**: Custom WebSocket connection management

### Data Processing
- **Pandas**: Data manipulation and analysis
- **NumPy**: Numerical computing
- **Construct**: Binary data parsing

### Development Tools
- **Docker & Docker Compose**: Containerization
- **RedisInsight**: Redis GUI for development
- **Ruff**: Fast Python linter
- **Python-dotenv**: Environment variable management

### Additional Libraries
- **Requests**: HTTP client
- **BeautifulSoup4**: HTML parsing
- **Curl-CFFI**: Enhanced HTTP client
- **Multitasking**: Concurrent task management
- **FrozenDict**: Immutable dictionary implementation

## Prerequisites

- Python 3.13+
- PostgreSQL database
- Redis Stack server
- Docker and Docker Compose (for containerized deployment)

## API Documentation

Once the application is running, visit:
- **Swagger UI**: http://localhost:8000
- **ReDoc**: http://localhost:8000/redoc

### Key Endpoints

#### Authentication
- `POST /auth/login` - User login
- `POST /auth/register` - User registration
- `GET /auth/google/login` - Google OAuth login

#### Exchanges
- `GET /exchange/` - List all exchanges
- `POST /exchange/` - Create new exchange
- `GET /exchange/{id}` - Get exchange by ID
- `PUT /exchange/{id}` - Update exchange
- `DELETE /exchange/{id}` - Delete exchange

#### Instruments
- `GET /instruments/` - List instruments
- `POST /instruments/` - Create instrument
- `GET /instruments/{id}` - Get instrument details

#### Market Data
- `GET /marketdata/intraday` - Get intraday price history
- `GET /marketdata/daily` - Get daily price history

#### WebSocket
- `WS /ws` - WebSocket endpoint for real-time data

### Request/Response Examples

#### Create Exchange
```http
POST /exchange/
Authorization: Bearer <jwt_token>
Content-Type: application/json

{
    "name": "NASDAQ",
    "code": "NASDAQ",
    "timezone": "America/New_York",
    "country": "USA",
    "currency": "USD",
    "market_open_time": "09:30:00",
    "market_close_time": "16:00:00"
}
```

#### Get Instruments
```http
GET /instruments/?exchange_id=1&limit=50
Authorization: Bearer <jwt_token>
```

#### Get Intraday Price History
```http
GET /marketdata/intraday?symbol=AAPL&start_date=2024-01-01&end_date=2024-01-02
Authorization: Bearer <jwt_token>
```

### Authentication Flow
1. **Registration**: `POST /auth/register` with email and password
2. **Login**: `POST /auth/login` returns JWT token
3. **Use Token**: Include `Authorization: Bearer <token>` in subsequent requests
4. **Google OAuth**: Redirect to `/auth/google/login` for OAuth flow

### Error Responses
All endpoints return standardized error responses:
```json
{
    "detail": "Error message",
    "status_code": 400
}
```
## Data Ingestion

The application supports real-time data ingestion from multiple providers:

### Supported Providers
- **Yahoo Finance (YF)**: Global markets, comprehensive data
- **Dhan (DHAN)**: Indian markets, NSE/BSE data

### Ingestion Process
1. **ProviderManager** initializes and manages provider connections
2. **LiveDataIngestion** handles incoming data streams
3. Data is processed in batches for optimal performance
4. **RedisTimeSeries** stores real-time data
5. **DataSaver** periodically persists data to PostgreSQL

### Exchange-Provider Mapping
The system uses flexible mapping between exchanges and providers:
- Multiple providers can serve the same exchange
- Automatic failover and load balancing
- Provider-specific symbol mapping

### Provider Implementation
- **BaseMarketDataProvider**: Abstract base class defining the interface for all providers
- **YahooFinanceProvider**: Uses yfinance library to fetch data from Yahoo Finance API
- **DhanProvider**: Integrates with Dhan API for Indian market data using client credentials

### Data Processing
- **DataIngestionFormat**: Standardized data structure for incoming market data
- **Binary Conversions**: Utilities for converting OHLCV data to binary format for efficient storage
- **Batch Processing**: Asynchronous queue-based processing to handle high-volume data streams

### Background Services
- **RedisSubscriber**: Listens for Redis pub/sub messages for cross-service communication
- **DataSaver**: Scheduled service that saves intraday data to database every 5 minutes
- **DataBroadcast**: (Commented out) Service for broadcasting data updates

## WebSocket Usage

### Connection
```javascript
const ws = new WebSocket('ws://localhost:8000/ws');
```

### Subscription
```javascript
ws.send(JSON.stringify({
    "message_type": "subscribe",
    "channel": "AAPL"  // Instrument symbol
}));
```

### Message Types
- **subscribe**: Subscribe to instrument updates
- **unsubscribe**: Unsubscribe from instrument
- **data**: Real-time price updates
- **info**: System messages

### Example Messages

#### Subscription Confirmation
```json
{
    "message_type": "info",
    "message": "Subscribed to AAPL"
}
```

#### Real-time Data Update
```json
{
    "message_type": "data",
    "symbol": "AAPL",
    "timestamp": "2024-01-15T16:00:00Z",
    "open": 185.00,
    "high": 186.50,
    "low": 184.50,
    "close": 185.50,
    "volume": 1000000,
    "previous_close": 184.00,
    "adj_close": 185.50,
    "delivery_percentage": 45.5
}
```

#### Unsubscription
```javascript
ws.send(JSON.stringify({
    "message_type": "unsubscribe",
    "channel": "AAPL"
}));
```

### WebSocket Manager
- **Connection Handling**: Manages WebSocket connections and subscriptions
- **Channel-based Broadcasting**: Broadcasts data to subscribed clients for specific instruments
- **Connection Limits**: Supports multiple concurrent connections

## Development

### Code Structure
- `features/`: Modular API endpoints
  - `auth/`: Authentication routes, schemas, and services (JWT, Google OAuth)
  - `exchange/`: Exchange CRUD operations with provider mappings
  - `instruments/`: Instrument management and symbol mappings
  - `marketdata/`: Historical price data retrieval (intraday/daily)
  - `populate_database/`: Database seeding endpoints
  - `provider/`: Provider management
  - `websocket/`: WebSocket routes for real-time subscriptions
- `models/`: Database models with relationships
- `services/`: Business logic and background tasks
  - `data/`: Data ingestion, saving, and broadcasting services
  - `provider/`: Provider implementations and manager
  - `redis_subscriber.py`: Redis pub/sub listener
  - `redis_timeseries.py`: Time series data operations
  - `websocket_manager.py`: WebSocket connection management
- `config/`: Configuration and settings
  - `database_config.py`: Async database session management
  - `logger.py`: Logging configuration
  - `redis_config.py`: Redis connection setup
  - `settings.py`: Pydantic-based application settings
- `utils/`: Helper functions and constants
  - `binary_conversions.py`: OHLCV to binary format utilities
  - `common_constants.py`: WebSocket message types, intervals, etc.
  - `ohlcv_to_binary.py`: Data serialization utilities

### Adding New Providers
1. Create provider class inheriting from `BaseMarketDataProvider`
2. Implement required methods: `connect()`, `disconnect()`, `subscribe()`
3. Add to `ProviderManager.providers`
4. Update database mappings

### Database Migrations
```bash
# Create new migration
alembic revision -m "description"

# Apply migrations
alembic upgrade head

# Rollback
alembic downgrade -1
```

### Testing
```bash
# Run tests (when implemented)
pytest

# Code linting
ruff check .
```

### Configuration Management
- **Pydantic Settings**: Environment-based configuration with validation
- **Async Database Sessions**: Connection pooling with SQLAlchemy
- **Redis Connections**: Centralized Redis client management
- **Logging**: Structured logging with configurable levels

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

### Code Standards
- Use type hints
- Follow PEP 8 style guide
- Use async/await for I/O operations
- Add docstrings to functions and classes
- Use meaningful variable names

### Commit Messages
- Use conventional commits format
- Example: `feat: add real-time data broadcasting`

---

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Support

For questions or issues:
- Create an issue on GitHub
- Check the API documentation
- Review the logs for error details
