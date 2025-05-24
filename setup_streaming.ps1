# Amazon Product Review Streaming Setup Script
# This script sets up the complete streaming environment for Amazon product review analysis

param(
    [switch]$SkipInstall,
    [switch]$TestOnly,
    [int]$ProducerDelay = 100,
    [int]$MaxRecords = 1000
)

Write-Host "=== Amazon Product Review Streaming Setup ===" -ForegroundColor Green

# Function to check if a command exists
function Test-Command($cmdname) {
    return [bool](Get-Command -Name $cmdname -ErrorAction SilentlyContinue)
}

# Function to check if a service is running
function Test-ServiceRunning($serviceName) {
    try {
        $service = Get-Service -Name $serviceName -ErrorAction SilentlyContinue
        return $service.Status -eq "Running"
    }
    catch {
        return $false
    }
}

# Function to wait for Kafka to be ready
function Wait-ForKafka {
    param([int]$TimeoutSeconds = 60)
    
    Write-Host "Waiting for Kafka to be ready..." -ForegroundColor Yellow
    $elapsed = 0
    
    while ($elapsed -lt $TimeoutSeconds) {
        try {
            # Test if Kafka is responding
            $testResult = python -c "
from kafka import KafkaProducer
import sys
try:
    producer = KafkaProducer(bootstrap_servers='localhost:9092', request_timeout_ms=5000)
    producer.close()
    print('SUCCESS')
except:
    print('FAILED')
    sys.exit(1)
" 2>$null
            
            if ($testResult -eq "SUCCESS") {
                Write-Host "✓ Kafka is ready!" -ForegroundColor Green
                return $true
            }
        }
        catch {
            # Continue waiting
        }
        
        Start-Sleep -Seconds 2
        $elapsed += 2
        Write-Host "." -NoNewline -ForegroundColor Yellow
    }
    
    Write-Host ""
    Write-Host "✗ Kafka failed to start within $TimeoutSeconds seconds" -ForegroundColor Red
    return $false
}

# Check prerequisites
Write-Host "Checking prerequisites..." -ForegroundColor Yellow

# Check Java
if (-not (Test-Command "java")) {
    Write-Host "ERROR: Java is not installed or not in PATH" -ForegroundColor Red
    Write-Host "Please install Java 8 or 11 and add to PATH" -ForegroundColor Red
    exit 1
}

# Check Python
if (-not (Test-Command "python")) {
    Write-Host "ERROR: Python is not installed or not in PATH" -ForegroundColor Red
    exit 1
}

# Check Docker (optional but recommended)
$dockerAvailable = Test-Command "docker"
if ($dockerAvailable) {
    Write-Host "✓ Docker is available" -ForegroundColor Green
} else {
    Write-Host "⚠ Docker not found - will use local installations" -ForegroundColor Yellow
}

# Create directory structure
Write-Host "Creating directory structure..." -ForegroundColor Yellow
$baseDir = "c:\Users\mot\Documents\Master\Bigdata\amazonProduct"
$dirs = @("kafka", "spark", "data", "logs", "scripts", "dashboard\templates", "dashboard\static")

foreach ($dir in $dirs) {
    $fullPath = Join-Path $baseDir $dir
    if (-not (Test-Path $fullPath)) {
        New-Item -ItemType Directory -Path $fullPath -Force | Out-Null
        Write-Host "Created directory: $fullPath" -ForegroundColor Green
    }
}

# Download Kafka if not exists
$kafkaDir = Join-Path $baseDir "kafka"
$kafkaVersion = "2.13-3.5.0"
$kafkaArchive = "kafka_$kafkaVersion.tgz"
$kafkaUrl = "https://archive.apache.org/dist/kafka/3.5.0/$kafkaArchive"

if (-not (Test-Path (Join-Path $kafkaDir "bin"))) {
    Write-Host "Downloading Kafka..." -ForegroundColor Yellow
    $kafkaPath = Join-Path $kafkaDir $kafkaArchive
    
    if (-not (Test-Path $kafkaPath)) {
        try {
            Invoke-WebRequest -Uri $kafkaUrl -OutFile $kafkaPath
            Write-Host "✓ Kafka downloaded" -ForegroundColor Green
        }
        catch {
            Write-Host "ERROR: Failed to download Kafka" -ForegroundColor Red
            Write-Host "Please download manually from: $kafkaUrl" -ForegroundColor Yellow
        }
    }
    
    # Extract Kafka (requires 7-Zip or similar)
    if (Test-Path $kafkaPath) {
        Write-Host "Extracting Kafka..." -ForegroundColor Yellow
        # Note: This requires 7-Zip or tar command
        if (Test-Command "tar") {
            tar -xzf $kafkaPath -C $kafkaDir --strip-components=1
            Write-Host "✓ Kafka extracted" -ForegroundColor Green
        } else {
            Write-Host "⚠ Please extract $kafkaPath manually to $kafkaDir" -ForegroundColor Yellow
        }
    }
}

# Download Spark if not exists
$sparkDir = Join-Path $baseDir "spark"
$sparkVersion = "3.4.1"
$sparkArchive = "spark-$sparkVersion-bin-hadoop3.tgz"
$sparkUrl = "https://archive.apache.org/dist/spark/spark-$sparkVersion/$sparkArchive"

if (-not (Test-Path (Join-Path $sparkDir "bin"))) {
    Write-Host "Downloading Spark..." -ForegroundColor Yellow
    $sparkPath = Join-Path $sparkDir $sparkArchive
    
    if (-not (Test-Path $sparkPath)) {
        try {
            Invoke-WebRequest -Uri $sparkUrl -OutFile $sparkPath
            Write-Host "✓ Spark downloaded" -ForegroundColor Green
        }
        catch {
            Write-Host "ERROR: Failed to download Spark" -ForegroundColor Red
            Write-Host "Please download manually from: $sparkUrl" -ForegroundColor Yellow
        }
    }
    
    # Extract Spark
    if (Test-Path $sparkPath) {
        Write-Host "Extracting Spark..." -ForegroundColor Yellow
        if (Test-Command "tar") {
            tar -xzf $sparkPath -C $sparkDir --strip-components=1
            Write-Host "✓ Spark extracted" -ForegroundColor Green
        } else {
            Write-Host "⚠ Please extract $sparkPath manually to $sparkDir" -ForegroundColor Yellow
        }
    }
}

# Install Python dependencies
Write-Host "Installing Python dependencies..." -ForegroundColor Yellow
$requirements = @(
    "kafka-python==2.0.2",
    "pymongo==4.5.0",
    "pandas==2.0.3",
    "numpy==1.24.3",
    "scikit-learn==1.3.0",
    "nltk==3.8.1",
    "flask==2.3.3",
    "flask-socketio==5.3.6",
    "python-dotenv==1.0.0",
    "pyspark==3.4.1"
)

foreach ($package in $requirements) {
    try {
        pip install $package
        Write-Host "✓ Installed $package" -ForegroundColor Green
    }
    catch {
        Write-Host "⚠ Failed to install $package" -ForegroundColor Yellow
    }
}

# Create environment file
Write-Host "Creating environment configuration..." -ForegroundColor Yellow
$envContent = @"
# MongoDB Configuration
MONGO_URI=mongodb://admin:admin@localhost:27017/
DB_NAME=amazon_reviews

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC=amazon_reviews

# Spark Configuration
SPARK_HOME=$sparkDir
SPARK_MASTER=local[*]

# Dashboard Configuration
FLASK_ENV=development
SECRET_KEY=your-secret-key-change-this
"@

$envFile = Join-Path $baseDir ".env"
$envContent | Out-File -FilePath $envFile -Encoding UTF8
Write-Host "✓ Environment file created: $envFile" -ForegroundColor Green

# Create Kafka startup script
$kafkaStartScript = Join-Path $baseDir "scripts\start_kafka.ps1"
$kafkaStartContent = @"
# Start Kafka Services
`$kafkaDir = "$kafkaDir"

Write-Host "Starting Zookeeper..." -ForegroundColor Yellow
Start-Process -FilePath (Join-Path `$kafkaDir "bin\windows\zookeeper-server-start.bat") -ArgumentList (Join-Path `$kafkaDir "config\zookeeper.properties") -NoNewWindow

Start-Sleep -Seconds 10

Write-Host "Starting Kafka Server..." -ForegroundColor Yellow
Start-Process -FilePath (Join-Path `$kafkaDir "bin\windows\kafka-server-start.bat") -ArgumentList (Join-Path `$kafkaDir "config\server.properties") -NoNewWindow

Start-Sleep -Seconds 5

Write-Host "Creating Kafka topic..." -ForegroundColor Yellow
& (Join-Path `$kafkaDir "bin\windows\kafka-topics.bat") --create --topic amazon_reviews --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

Write-Host "✓ Kafka setup complete!" -ForegroundColor Green
"@

$kafkaStartContent | Out-File -FilePath $kafkaStartScript -Encoding UTF8
Write-Host "✓ Kafka startup script created: $kafkaStartScript" -ForegroundColor Green

# Create MongoDB startup script (if using Docker)
if ($dockerAvailable) {
    $mongoStartScript = Join-Path $baseDir "scripts\start_mongodb.ps1"
    $mongoStartContent = @"
# Start MongoDB using Docker
Write-Host "Starting MongoDB with Docker..." -ForegroundColor Yellow

docker run -d ``
  --name mongodb-amazon ``
  -p 27017:27017 ``
  -e MONGO_INITDB_ROOT_USERNAME=admin ``
  -e MONGO_INITDB_ROOT_PASSWORD=admin ``
  -v mongodb_data:/data/db ``
  mongo:5.0

Write-Host "✓ MongoDB started on localhost:27017" -ForegroundColor Green
Write-Host "Username: admin, Password: admin" -ForegroundColor Yellow
"@

    $mongoStartContent | Out-File -FilePath $mongoStartScript -Encoding UTF8
    Write-Host "✓ MongoDB startup script created: $mongoStartScript" -ForegroundColor Green
}

# Create complete startup script
$startAllScript = Join-Path $baseDir "start_all.ps1"
$startAllContent = @"
# Start All Services for Amazon Review Streaming
Write-Host "=== Starting Amazon Review Streaming Environment ===" -ForegroundColor Green

# Start MongoDB (if using Docker)
if (Get-Command "docker" -ErrorAction SilentlyContinue) {
    Write-Host "Starting MongoDB..." -ForegroundColor Yellow
    & "$baseDir\scripts\start_mongodb.ps1"
    Start-Sleep -Seconds 5
}

# Start Kafka
Write-Host "Starting Kafka..." -ForegroundColor Yellow
& "$baseDir\scripts\start_kafka.ps1"
Start-Sleep -Seconds 10

# Start the streaming components in separate windows
Write-Host "Starting streaming components..." -ForegroundColor Yellow

# Start Kafka producer
Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd '$baseDir'; python kafka_producer.py"

# Start Kafka consumer  
Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd '$baseDir'; python kafka_consumer.py"

# Start Spark streaming
Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd '$baseDir'; python spark_streaming.py"

# Start Flask dashboard
Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd '$baseDir\dashboard'; python app.py"

Write-Host "✓ All services started!" -ForegroundColor Green
Write-Host "Dashboard available at: http://localhost:5001" -ForegroundColor Cyan
Write-Host "MongoDB: localhost:27017 (admin/admin)" -ForegroundColor Cyan
Write-Host "Kafka: localhost:9092" -ForegroundColor Cyan
"@

$startAllContent | Out-File -FilePath $startAllScript -Encoding UTF8
Write-Host "✓ Complete startup script created: $startAllScript" -ForegroundColor Green

# Create sample data file
Write-Host "Creating sample data..." -ForegroundColor Yellow
$sampleDataFile = Join-Path $baseDir "data\sample_reviews.json"
$sampleData = @"
[
    {"asin": "B00004Y2UT", "reviewText": "This product is amazing! Great quality and fast delivery.", "overall": 5.0, "reviewerID": "A1", "reviewerName": "John Doe", "summary": "Excellent product", "unixReviewTime": 1640995200, "reviewTime": "01 1, 2022"},
    {"asin": "B00005Y2UX", "reviewText": "Not what I expected. Poor quality and overpriced.", "overall": 2.0, "reviewerID": "A2", "reviewerName": "Jane Smith", "summary": "Disappointed", "unixReviewTime": 1640995260, "reviewTime": "01 1, 2022"},
    {"asin": "B00006Y2UZ", "reviewText": "Average product. Works as described but nothing special.", "overall": 3.0, "reviewerID": "A3", "reviewerName": "Bob Johnson", "summary": "Okay product", "unixReviewTime": 1640995320, "reviewTime": "01 1, 2022"},
    {"asin": "B00007Y2UA", "reviewText": "Fantastic! Exceeded my expectations in every way.", "overall": 5.0, "reviewerID": "A4", "reviewerName": "Alice Brown", "summary": "Love it!", "unixReviewTime": 1640995380, "reviewTime": "01 1, 2022"},
    {"asin": "B00008Y2UB", "reviewText": "Terrible product. Broke after one day of use.", "overall": 1.0, "reviewerID": "A5", "reviewerName": "Charlie Wilson", "summary": "Waste of money", "unixReviewTime": 1640995440, "reviewTime": "01 1, 2022"}
]
"@

$sampleData | Out-File -FilePath $sampleDataFile -Encoding UTF8
Write-Host "✓ Sample data created: $sampleDataFile" -ForegroundColor Green

# Final instructions
Write-Host "`n=== Setup Complete! ===" -ForegroundColor Green
Write-Host "Next steps:" -ForegroundColor Yellow
Write-Host "1. Run: .\start_all.ps1 to start all services" -ForegroundColor Cyan
Write-Host "2. Open browser to: http://localhost:5001 for the dashboard" -ForegroundColor Cyan
Write-Host "3. Monitor the console windows for each service" -ForegroundColor Cyan
Write-Host "`nTo stop services:" -ForegroundColor Yellow
Write-Host "- Close the PowerShell windows" -ForegroundColor Cyan
Write-Host "- Stop Docker containers if using MongoDB in Docker" -ForegroundColor Cyan
Write-Host "`nLog files will be created in: $baseDir\logs" -ForegroundColor Cyan

# Create a quick test script
$testScript = Join-Path $baseDir "test_setup.ps1"
$testContent = @"
# Test the streaming setup
Write-Host "Testing Amazon Review Streaming Setup..." -ForegroundColor Green

# Test Python imports
python -c "import kafka, pymongo, flask, pyspark; print('✓ All Python packages imported successfully')"

# Test Kafka directory
if (Test-Path "$kafkaDir\bin") {
    Write-Host "✓ Kafka installation found" -ForegroundColor Green
} else {
    Write-Host "✗ Kafka installation not found" -ForegroundColor Red
}

# Test Spark directory  
if (Test-Path "$sparkDir\bin") {
    Write-Host "✓ Spark installation found" -ForegroundColor Green
} else {
    Write-Host "✗ Spark installation not found" -ForegroundColor Red
}

Write-Host "Test complete!" -ForegroundColor Green
"@

$testContent | Out-File -FilePath $testScript -Encoding UTF8
Write-Host "✓ Test script created: $testScript" -ForegroundColor Green

Write-Host "`nSetup script completed successfully!" -ForegroundColor Green
