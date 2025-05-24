#!/usr/bin/env pwsh
# Amazon Product Review Streaming Setup Script (PowerShell Core)
# This script sets up and runs the Kafka streaming environment

param(
    [switch]$SkipInstall,
    [switch]$TestOnly,
    [switch]$StopServices,
    [int]$ProducerDelay = 100,
    [int]$MaxRecords = 1000,
    [switch]$Verbose
)

Write-Host "=== Amazon Product Review Streaming Setup ===" -ForegroundColor Green

function Test-Command($cmdname) {
    return [bool](Get-Command -Name $cmdname -ErrorAction SilentlyContinue)
}

function Wait-ForKafka {
    param([int]$TimeoutSeconds = 60)
    
    Write-Host "Waiting for Kafka to be ready..." -ForegroundColor Yellow
    $elapsed = 0
    
    while ($elapsed -lt $TimeoutSeconds) {
        try {
            $testResult = python -c @"
from kafka import KafkaProducer
import sys
try:
    producer = KafkaProducer(bootstrap_servers='localhost:9092', request_timeout_ms=5000)
    producer.close()
    print('SUCCESS')
except Exception as e:
    print(f'FAILED: {e}')
    sys.exit(1)
"@ 2>$null
            
            if ($testResult -eq "SUCCESS") {
                Write-Host "`n✓ Kafka is ready!" -ForegroundColor Green
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
    
    Write-Host "`n✗ Kafka failed to start within $TimeoutSeconds seconds" -ForegroundColor Red
    return $false
}

# Stop services if requested
if ($StopServices) {
    Write-Host "Stopping services..." -ForegroundColor Yellow
    docker-compose down
    Write-Host "✓ Services stopped" -ForegroundColor Green
    exit 0
}

# Check prerequisites
Write-Host "Checking prerequisites..." -ForegroundColor Yellow

$missing = @()
if (-not (Test-Command "docker")) { $missing += "Docker" }
if (-not (Test-Command "docker-compose")) { $missing += "Docker Compose" }
if (-not (Test-Command "python")) { $missing += "Python" }

if ($missing.Count -gt 0) {
    Write-Host "ERROR: Missing prerequisites: $($missing -join ', ')" -ForegroundColor Red
    Write-Host "Please install the missing components and try again." -ForegroundColor Red
    exit 1
}

Write-Host "✓ All prerequisites found" -ForegroundColor Green

# Check required files
$requiredFiles = @(
    "kafka_producer.py",
    "kafka_consumer.py", 
    "Data.json",
    "requirements.txt",
    "docker-compose.yml"
)

$missingFiles = @()
foreach ($file in $requiredFiles) {
    if (-not (Test-Path $file)) {
        $missingFiles += $file
    }
}

if ($missingFiles.Count -gt 0) {
    Write-Host "ERROR: Missing required files: $($missingFiles -join ', ')" -ForegroundColor Red
    exit 1
}

Write-Host "✓ All required files found" -ForegroundColor Green

# Install Python dependencies
if (-not $SkipInstall) {
    Write-Host "Installing Python dependencies..." -ForegroundColor Yellow
    try {
        pip install -r requirements.txt
        Write-Host "✓ Python dependencies installed" -ForegroundColor Green
    }
    catch {
        Write-Host "ERROR: Failed to install Python dependencies" -ForegroundColor Red
        Write-Host "Please run: pip install -r requirements.txt" -ForegroundColor Yellow
        exit 1
    }
}

# Start Kafka services
Write-Host "Starting Kafka services..." -ForegroundColor Yellow
try {
    docker-compose up -d
    Write-Host "✓ Docker services started" -ForegroundColor Green
}
catch {
    Write-Host "ERROR: Failed to start Docker services" -ForegroundColor Red
    exit 1
}

# Wait for Kafka to be ready
if (-not (Wait-ForKafka -TimeoutSeconds 90)) {
    Write-Host "ERROR: Kafka failed to start properly" -ForegroundColor Red
    Write-Host "Check Docker logs: docker-compose logs kafka" -ForegroundColor Yellow
    exit 1
}

# Create Kafka topic
Write-Host "Creating Kafka topic..." -ForegroundColor Yellow
try {
    docker exec kafka kafka-topics --create --topic amazon-reviews --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1 --if-not-exists
    Write-Host "✓ Kafka topic created" -ForegroundColor Green
}
catch {
    Write-Host "⚠ Topic creation failed (might already exist)" -ForegroundColor Yellow
}

if ($TestOnly) {
    Write-Host "Running test..." -ForegroundColor Yellow
    python test_kafka.py
    exit $LASTEXITCODE
}

# Show usage instructions
Write-Host "`n=== Setup Complete! ===" -ForegroundColor Green
Write-Host "Kafka is running and ready to use.`n" -ForegroundColor Green

Write-Host "Usage Examples:" -ForegroundColor Cyan
Write-Host "1. Start Producer (in one terminal):" -ForegroundColor White
Write-Host "   python kafka_producer.py --max-records 1000 --delay $ProducerDelay" -ForegroundColor Gray
Write-Host ""
Write-Host "2. Start Consumer (in another terminal):" -ForegroundColor White  
Write-Host "   python kafka_consumer.py --stats-interval 5" -ForegroundColor Gray
Write-Host ""
Write-Host "3. Run quick test:" -ForegroundColor White
Write-Host "   python test_kafka.py" -ForegroundColor Gray
Write-Host ""
Write-Host "4. View Kafka logs:" -ForegroundColor White
Write-Host "   docker-compose logs -f kafka" -ForegroundColor Gray
Write-Host ""
Write-Host "5. Stop services:" -ForegroundColor White
Write-Host "   docker-compose down" -ForegroundColor Gray
Write-Host ""

# Offer to run a quick demo
$response = Read-Host "Would you like to run a quick demo now? (y/N)"
if ($response -eq "y" -or $response -eq "Y") {
    Write-Host "`nStarting quick demo..." -ForegroundColor Yellow
    
    # Start consumer in background
    Write-Host "Starting consumer..." -ForegroundColor Yellow
    $consumerJob = Start-Job -ScriptBlock {
        Set-Location $using:PWD
        python kafka_consumer.py --max-messages 50 --stats-interval 3
    }
    
    Start-Sleep -Seconds 3
    
    # Start producer
    Write-Host "Starting producer..." -ForegroundColor Yellow
    python kafka_producer.py --max-records 50 --delay 200 --verbose
    
    # Wait for consumer to finish
    Write-Host "Waiting for consumer to finish..." -ForegroundColor Yellow
    Wait-Job $consumerJob -Timeout 30 | Out-Null
    Receive-Job $consumerJob
    Remove-Job $consumerJob -Force
    
    Write-Host "`n✓ Demo completed!" -ForegroundColor Green
}

Write-Host "`nSetup script completed successfully!" -ForegroundColor Green
