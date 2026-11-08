# Currency Exchange Rate ETL Pipeline

---

## 1. Project Overview

This project implements an automated Extract, Transform, and Load (ETL) pipeline using Apache Airflow to retrieve currency exchange rates from the Frankfurter API, transform the retrieved data into a structured format, and store the results in MongoDB.

The pipeline retrieves exchange rates for USD, GBP, and JPY against EUR. Airflow manages the execution and dependencies of the ETL tasks, while MongoDB provides persistent storage for the processed records. Mongo Express is included as a web-based interface for managing and viewing the MongoDB database.

## 2. Technologies Used

- **Apache Airflow:** Orchestration and scheduling of ETL tasks.
- **Astro CLI:** Local Airflow project development and execution.
- **Python:** Implementation of extraction, transformation, and loading logic.
- **Frankfurter API:** Source of currency exchange rate data.
- **MongoDB:** NoSQL database for storing transformed exchange rates.
- **Mongo Express:** Web-based MongoDB administration interface.
- **Docker & Docker Compose:** Containerization, service management, and networking.
- **XCom:** Transfer of data between Airflow tasks.

## 3. ETL Workflow

The pipeline consists of three sequential Airflow tasks:

### Task 1: Extract Currency Exchange Rates

The extraction task sends an HTTP GET request to the Frankfurter API:

`https://api.frankfurter.dev/v2/rates`

The request uses:
- Base currency: EUR
- Target currencies: USD, GBP, JPY

The API response is retrieved as JSON and stored in Airflow XCom using the key `raw_rates`.

### Task 2: Transform Exchange Rate Data

The transformation task retrieves the extracted records through XCom and converts them into a structured format.

Each transformed record contains:

| Field | Description |
|---|---|
| base_currency | Original currency (EUR) |
| target_currency | Target currency (USD, GBP, JPY) |
| exchange_rate | Retrieved exchange rate |
| rate_date | Date of the exchange rate |
| source | Frankfurter API |
| retrieved_at | Timestamp of data retrieval |

The transformed records are stored in XCom using the key `transformed_rates`.

### Task 3: Load Data into MongoDB

The loading task retrieves the transformed records from XCom and inserts or updates them in MongoDB.

The database configuration is:

- **Database:** `currency_db`
- **Collection:** `currency_rates`

MongoDB uses an upsert operation to avoid creating duplicate records for the same base currency, target currency, and rate date.

## 4. Airflow DAG Configuration

The pipeline is implemented in:

`dags/currency_exchange_etl.py`

**DAG ID:** `currency_exchange_etl`

**Schedule:** Daily (`@daily`)

**Execution order:**

```text
extract_currency_rates
          |
          v
transform_exchange_rate_data
          |
          v
load_data_into_mongodb
```

The tasks are connected through Airflow dependencies, ensuring that transformation starts after extraction and loading starts after transformation.

## 5. Docker Services and Persistent Storage

Docker Compose is used to run MongoDB and Mongo Express alongside the Airflow environment.

| Service | Purpose | Port |
|---|---|---|
| Airflow | ETL orchestration | 8080 |
| MongoDB | Database storage | 27017 |
| Mongo Express | Database management UI | 8081 |

**Docker Networking:** MongoDB is accessible to Airflow through the Docker network using the service hostname `mongo`.

**Persistent Storage:** A named Docker volume, `mongo_data`, is mounted at `/data/db` to preserve MongoDB records across container restarts.

## 6. Project Structure

```text
21L-7990_A3_Airflow_ETL/
│
├── dags/
│   ├── currency_exchange_etl.py
│   └── exampledag.py
│
├── tests/
│   └── dags/
│       └── test_dag_example.py
│
├── docker-compose.override.yml
├── Dockerfile
├── requirements.txt
├── packages.txt
├── .env.example
├── .gitignore
├── .dockerignore
└── README.md
```

## 7. Setup and Execution

### Prerequisites

Install the following tools:

- Docker Desktop
- Astro CLI
- Git
- Visual Studio Code

### Step 1: Clone the Repository

```bash
git clone https://github.com/USERNAME/Task-3_Airflow_ETL.git
cd Task-3_Airflow_ETL
```

### Step 2: Configure Environment Variables

Create a local `.env` file using `.env.example` as a template.

```env
MONGO_INITDB_ROOT_USERNAME=your_username
MONGO_INITDB_ROOT_PASSWORD=your_password
MONGO_EXPRESS_USERNAME=your_username
MONGO_EXPRESS_PASSWORD=your_password
MONGO_URI=mongodb://your_username:your_password@mongo:27017/?authSource=admin
```

Use matching MongoDB credentials in the MongoDB URI and environment variables.


### Step 3: Install Python Dependencies

The project dependencies are listed in `requirements.txt` and are installed in the Airflow environment.

### Step 4: Start the Airflow Project

```bash
astro dev start
```

### Step 5: Access the Services

- Airflow: http://localhost:8080
- Mongo Express: http://localhost:8081

### Step 6: Execute the DAG

1. Open the Airflow web interface.
2. Locate `currency_exchange_etl`.
3. Unpause the DAG if necessary.
4. Trigger the DAG manually.
5. Monitor the three tasks through the Graph or Grid view.

### Step 7: Verify MongoDB Records

Open Mongo Express and navigate to:

`currency_db` → `currency_rates`

The collection should contain the processed exchange rate records.

## 8. Execution Results

The ETL pipeline was executed successfully.

- The Extract task retrieved exchange rate records from the Frankfurter API.
- The Transform task converted the API response into the required structured format.
- The Load task successfully stored the transformed records in MongoDB.
- All three Airflow tasks completed successfully.
- Mongo Express was used to verify the stored records.

## 9. Conclusion

This project demonstrates the implementation of an automated ETL pipeline using Apache Airflow, Python, and MongoDB. It integrates an external currency exchange API, uses XCom for communication between Airflow tasks, and employs Docker networking and persistent storage for database management.

The successful execution of all three tasks and verification of the stored records demonstrate that the pipeline performs the required extraction, transformation, and loading operations.

---
