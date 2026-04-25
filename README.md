# Retail Customer Segmentation Backend

A FastAPI-based backend for customer segmentation analysis using RFM (Recency, Frequency, Monetary) scoring and machine learning clustering.

## Project Overview

This project performs customer segmentation analysis on retail sales data with the following capabilities:

- **Data Processing**: Clean and preprocess raw retail data
- **RFM Analysis**: Calculate Recency, Frequency, and Monetary metrics
- **Clustering**: Use K-means to segment customers into groups
- **API**: RESTful API to serve segmentation results
- **Insights**: AI-powered recommendations for each customer segment
- **Visualization**: Dashboard integration and report generation

## Project Structure

```
├── app/                          # Main application package
│   ├── __init__.py
│   ├── main.py                   # FastAPI application entry point
│   ├── api/                      # API routes
│   │   ├── routes/
│   │   │   ├── health.py         # Health check endpoints
│   │   │   ├── dataset.py        # Dataset management endpoints
│   │   │   ├── segmentation.py   # Segmentation endpoints
│   │   │   ├── dashboard.py      # Dashboard endpoints
│   │   │   └── insights.py       # AI insights endpoints
│   ├── core/                     # Core configurations
│   │   └── config.py             # Settings and configuration
│   ├── db/                       # Database setup
│   │   ├── base.py               # Base models
│   │   └── session.py            # Database session management
│   ├── models/                   # Database models
│   ├── schemas/                  # Pydantic schemas for API
│   └── services/                 # Business logic services
├── data/
│   ├── raw/                      # Raw retail data
│   └── processed/                # Processed data
├── scripts/
│   ├── preprocessing/            # Data cleaning scripts
│   ├── segmentation/             # Segmentation scripts
│   └── utils/                    # Utility functions
├── notebooks/                    # Jupyter notebooks for analysis
├── reports/                      # Generated reports
├── requirements.txt              # Project dependencies
├── .gitignore                    # Git ignore rules
└── README.md                     # This file
```

## Installation

### Prerequisites

- Python 3.9 or higher
- pip or conda

### Setup

1. **Clone the repository** (if applicable):
   ```bash
   git clone <repository-url>
   cd Retail_Customer_Segmentation_Backend
   ```

2. **Create virtual environment**:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

4. **Create .env file** (optional):
   ```bash
   cp .env.example .env
   ```

## Configuration

Edit `app/core/config.py` to customize settings:

```python
# App Settings
app_name = "Retail Customer Segmentation API"
app_version = "1.0.0"

# Database
database_url = "sqlite:///./segmentation.db"

# Data Paths
raw_data_path = "data/raw"
processed_data_path = "data/processed"

# Clustering
n_clusters = 4
random_state = 42
```

## Running the API

### Development Server

```bash
python app/main.py
```

Or using Uvicorn directly:

```bash
uvicorn app.main:app --reload
```

The API will be available at `http://localhost:8000`

- **API Documentation (Swagger UI)**: http://localhost:8000/docs
- **Alternative Documentation (ReDoc)**: http://localhost:8000/redoc

### Production Server

```bash
uvicorn app.main:app --host 0.0.0.0 --port 8000 --workers 4
```

## API Endpoints

### Health Check

- `GET /api/v1/health` - Health check endpoint
- `GET /api/v1/` - Root endpoint

### Datasets

- `GET /api/v1/datasets/` - List all datasets
- `POST /api/v1/datasets/upload` - Upload new dataset
- `GET /api/v1/datasets/{dataset_id}` - Get dataset details

### Segmentation

- `POST /api/v1/segmentation/segment` - Create segmentation
- `GET /api/v1/segmentation/results/{segmentation_id}` - Get segmentation results
- `GET /api/v1/segmentation/segments` - List all segments

### Dashboard

- `GET /api/v1/dashboard/overview` - Dashboard overview
- `GET /api/v1/dashboard/segment-distribution` - Segment distribution
- `GET /api/v1/dashboard/metrics` - Key metrics

### Insights

- `GET /api/v1/insights/recommendations/{segment_id}` - Get segment recommendations
- `POST /api/v1/insights/generate` - Generate insights
- `GET /api/v1/insights/summary` - Insights summary

## Data Pipeline

### Step 1: Data Preprocessing

The preprocessing scripts clean and prepare raw retail data:

```bash
python scripts/preprocessing/clean_data_step1.py
python scripts/preprocessing/clean_data_step2.py
python scripts/preprocessing/explore_data.py
python scripts/preprocessing/feature_engineering.py
```

### Step 2: RFM Analysis

Build RFM metrics for customer segmentation:

```bash
python scripts/segmentation/build_rfm_base.py
python scripts/segmentation/calculate_recency.py
python scripts/segmentation/rfm_scoring.py
```

### Step 3: Clustering

Perform K-means clustering:

```bash
python scripts/segmentation/clustering.py
```

### Step 4: Results

Generate customer summaries and segment mapping:

```bash
python scripts/segmentation/customer_summary.py
python scripts/segmentation/segment_mapping.py
```

## Dependencies

### Core Framework
- **FastAPI** - Modern API framework
- **Uvicorn** - ASGI server

### Data Processing
- **Pandas** - Data manipulation
- **NumPy** - Numerical computations
- **SciPy** - Scientific computing

### Machine Learning
- **scikit-learn** - Clustering and preprocessing

### Database
- **SQLAlchemy** - ORM
- **Alembic** - Database migrations

### Visualization
- **Matplotlib** - Static plots
- **Seaborn** - Statistical visualization
- **Plotly** - Interactive visualizations

### AI Integration
- **OpenAI** - GPT-based insights
- **Anthropic** - Claude-based insights

### Testing & Development
- **pytest** - Testing framework
- **black** - Code formatting
- **flake8** - Linting

## Usage Examples

### Using Python Directly

```python
from app.main import app
from app.core.config import settings
import pandas as pd

# Load and process data
data = pd.read_csv(settings.raw_data_path + '/online_retail.csv')

# Access API endpoints programmatically
from fastapi.testclient import TestClient

client = TestClient(app)
response = client.get("/api/v1/health")
print(response.json())
```

### Using cURL

```bash
# Health check
curl http://localhost:8000/api/v1/health

# Get segments
curl http://localhost:8000/api/v1/segmentation/segments

# Get insights
curl http://localhost:8000/api/v1/insights/summary
```

## Development Workflow

### Code Formatting

```bash
black app/ scripts/
isort app/ scripts/
```

### Linting

```bash
flake8 app/ scripts/
```

### Testing

```bash
pytest -v
pytest --cov=app  # With coverage
```

## Troubleshooting

### Import Errors

If you encounter import errors, ensure:
1. Virtual environment is activated
2. `requirements.txt` dependencies are installed
3. Python version is 3.9+

### Database Errors

Reset the database:
```bash
rm segmentation.db
python -c "from app.db import Base, engine; Base.metadata.create_all(bind=engine)"
```

### Port Already in Use

Change the port in `app/core/config.py` or use:
```bash
uvicorn app.main:app --port 8001
```

## Contributing

1. Create a feature branch
2. Make changes and run tests
3. Format code with `black` and `isort`
4. Submit a pull request

## Next Steps (Day 2+)

- [ ] Implement database models for customers and segments
- [ ] Add request/response schemas for API endpoints
- [ ] Implement data processing services
- [ ] Add clustering service
- [ ] Integrate AI insights generation
- [ ] Create comprehensive tests
- [ ] Set up CI/CD pipeline
- [ ] Deploy to production

## License

[Add your license here]

## Contact

[Add contact information]

## Resources

- [FastAPI Documentation](https://fastapi.tiangolo.com/)
- [SQLAlchemy Documentation](https://docs.sqlalchemy.org/)
- [scikit-learn Documentation](https://scikit-learn.org/)
- [Pydantic Documentation](https://docs.pydantic.dev/)
