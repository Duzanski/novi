# Oil & Gas Wells ETL Pipeline

A modern, scalable ETL pipeline for processing oil and gas well data from CSV/XML sources into a PostgreSQL database.

## 🚀 Features

- **Modular Architecture**: Clean separation of concerns with dedicated processors for wells, production, and database operations
- **Comprehensive Data Quality**: Built-in validation and cleaning rules for production data
- **Well Spacing Analysis**: Advanced geospatial analysis for well neighborhood characteristics
- **Robust Error Handling**: Comprehensive logging and error management
- **Type Safety**: Full type hints for better code reliability
- **Testing**: Unit tests with pytest and coverage reporting
- **Development Tools**: Linting, formatting, and type checking

## 📊 Data Processing

The pipeline processes:
- **Wells Data**: Location, operator, well type, and PLSS coordinates
- **Bottom Hole Locations**: Coordinates for lateral length calculations  
- **Production Data**: Monthly oil, gas, and water production volumes
- **Well Spacing**: Neighborhood analysis and density calculations

## 🏗️ Architecture

```
src/
├── __init__.py
├── config.py              # Configuration management
├── utils.py               # Utility functions (distance calculation, logging)
├── data_processors.py     # Core data processing classes
├── well_spacing.py        # Well spacing analysis (Task 3)
└── pipeline.py            # Main ETL orchestrator

tests/
├── __init__.py
└── test_utils.py          # Unit tests

main.py                    # Entry point
Makefile                   # Development commands
pyproject.toml            # Modern Python project configuration
```

## 🛠️ Setup

### Prerequisites
- Python 3.8+
- PostgreSQL database
- Virtual environment (recommended)

### Installation

1. **Clone and setup environment:**
```bash
git clone <repository>
cd data-recruitment-assignment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

2. **Install dependencies:**
```bash
make install-dev  # For development
# OR
make install      # For production only
```

3. **Configure database:**
Create a `.env` file with your database connection:
```env
DATABASE_CONNECTION=postgresql://username:password@localhost:5432/database_name
```

## 🚀 Usage

### Run the Pipeline
```bash
# Standard run
make run

# With debug logging
make run-debug

# Direct Python execution
python main.py --log-level INFO
```

### Development Commands
```bash
make help         # Show all available commands
make test         # Run tests with coverage
make lint         # Check code style
make format       # Format code with black
make type-check   # Run mypy type checking
make check        # Run all checks (lint + type + test)
make clean        # Clean generated files
```

## 📋 Tasks Completed

### ✅ Task 1: Bug Fix
**Issue**: Production graph showed incorrect timeline due to using `Received` date instead of `ReportPeriod`.

**Solution**: Changed date field from `Received` to `ReportPeriod` in production processing:
```python
# Fixed in src/data_processors.py
self.production_df["Date"] = self.production_df["ReportPeriod"]
```

### ✅ Task 2: PLSS Column & Cumulative Production
**PLSS Format**: Added Public Land Survey System coordinates in format "SWNE 24 3S 6W"
```python
wells["PLSS"] = (
    wells["QuarterQuarter"].astype(str) + " " +
    wells["Sec"].astype(str) + " " +
    wells["Township"].astype(str) + wells["TownshipDir"].astype(str) + " " +
    wells["Range"].astype(str) + wells["RangeDir"].astype(str)
)
```

**Cumulative Production**: Added calculated lifetime totals:
- `CumulativeOil_Calculated`
- `CumulativeGas_Calculated` 
- `CumulativeWater_Calculated`

### ✅ Task 3: Well Spacing Features
Implemented comprehensive well spacing analysis with multiple metrics:

1. **DistanceToNearestHorizontalWell**: Distance in feet to closest horizontal well
2. **HorizontalWellsWithin1Mile**: Count of horizontal wells within 1-mile radius
3. **AvgDistanceTo3NearestHorizontal**: Average distance to 3 nearest horizontal wells
4. **WellDensityScore**: Wells per square mile within 2-mile radius

```python
# Usage
from src.well_spacing import WellSpacingAnalyzer

analyzer = WellSpacingAnalyzer(wells_df)
wells_with_spacing = analyzer.add_spacing_features()
```

## 🔧 Key Improvements Made

### 1. **Code Structure & Maintainability**
- ❌ **Before**: Single 181-line script with Jupyter cells
- ✅ **After**: Modular classes with single responsibilities
- ✅ **Benefits**: Easier testing, debugging, and feature additions

### 2. **Configuration Management**
- ❌ **Before**: Hard-coded values scattered throughout
- ✅ **After**: Centralized configuration in `src/config.py`
- ✅ **Benefits**: Easy environment-specific settings

### 3. **Error Handling & Logging**
- ❌ **Before**: No error handling or logging
- ✅ **After**: Comprehensive logging and graceful error handling
- ✅ **Benefits**: Better debugging and monitoring capabilities

### 4. **Data Quality & Validation**
- ❌ **Before**: Basic data cleaning
- ✅ **After**: Comprehensive validation with industry-standard limits
- ✅ **Benefits**: More reliable data and quality insights

### 5. **Testing Infrastructure**
- ❌ **Before**: No tests
- ✅ **After**: Unit tests with pytest and coverage reporting
- ✅ **Benefits**: Confidence in code changes and refactoring

### 6. **Development Workflow**
- ❌ **Before**: No development tools
- ✅ **After**: Linting, formatting, type checking, and automation
- ✅ **Benefits**: Consistent code quality and faster development

### 7. **Scalability Improvements**
- ✅ **Vectorized operations** where possible
- ✅ **Efficient data filtering** and memory usage
- ✅ **Modular design** for easy extension to multiple states
- ✅ **Configuration-driven** approach for different environments

### 8. **Documentation & Usability**
- ❌ **Before**: Minimal documentation
- ✅ **After**: Comprehensive docstrings, type hints, and README
- ✅ **Benefits**: Easier onboarding and maintenance

## 📈 Performance Considerations

For production deployment with larger datasets:

1. **Chunk Processing**: Process large CSV/XML files in chunks
2. **Database Optimization**: Use bulk inserts and indexing
3. **Parallel Processing**: Leverage multiprocessing for independent calculations
4. **Caching**: Cache expensive calculations like distance matrices
5. **Memory Management**: Use generators for large datasets

## 🧪 Testing

```bash
# Run all tests
make test

# Run specific test file
pytest tests/test_utils.py -v

# Run with coverage
pytest --cov=src --cov-report=html
```

## 📊 Output Tables

### Wells Table
| Column | Description |
|--------|-------------|
| API10 | 10-digit API well identifier |
| State | State code |
| Operator | Operating company |
| IsHorizontalWell | Boolean flag for horizontal wells |
| SHLLatitude/SHLLongitude | Surface hole location |
| BHLatitude/BHLongitude | Bottom hole location |
| LateralLength | Calculated lateral length in feet |
| PLSS | Public Land Survey System coordinates |
| CumulativeOil_Calculated | Lifetime oil production (bbls) |
| CumulativeGas_Calculated | Lifetime gas production (mcf) |
| CumulativeWater_Calculated | Lifetime water production (bbls) |
| DistanceToNearestHorizontalWell | Distance to nearest horizontal well (ft) |
| HorizontalWellsWithin1Mile | Count of horizontal wells within 1 mile |
| AvgDistanceTo3NearestHorizontal | Average distance to 3 nearest horizontal wells |
| WellDensityScore | Wells per square mile within 2-mile radius |

### Production Table  
| Column | Description |
|--------|-------------|
| API10 | 10-digit API well identifier |
| Date | Production month (ReportPeriod) |
| Oil | Monthly oil production (bbls) |
| Gas | Monthly gas production (mcf) |
| Water | Monthly water production (bbls) |

## 🤝 Contributing

1. **Code Style**: Use `black` for formatting and `flake8` for linting
2. **Type Hints**: Add type hints to all functions
3. **Testing**: Write tests for new features
4. **Documentation**: Update docstrings and README

## 📝 License

MIT License - see LICENSE file for details.

---

**Next Steps for Production:**
1. Add data lineage tracking
2. Implement incremental updates
3. Add data quality dashboards  
4. Set up CI/CD pipeline
5. Add monitoring and alerting 