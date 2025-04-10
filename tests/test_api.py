import json
import pytest
import requests

# Test configuration
BASE_URL = "http://localhost:8000"  # Adjust based on your setup

def test_create_printer():
    """Test creating a printer."""
    data = {
        "company": "Creality",
        "model": "Ender 3"
    }
    
    response = requests.post(f"{BASE_URL}/api/v1/printers", json=data)
    assert response.status_code == 201
    
    printer = response.json()
    assert printer["company"] == "Creality"
    assert printer["model"] == "Ender 3"
    assert "id" in printer

def test_list_printers():
    """Test listing printers."""
    response = requests.get(f"{BASE_URL}/api/v1/printers")
    assert response.status_code == 200
    
    printers = response.json()
    assert isinstance(printers, list)

def test_create_filament():
    """Test creating a filament."""
    data = {
        "type": "PLA",
        "color": "Blue",
        "total_weight_in_grams": 1000
    }
    
    response = requests.post(f"{BASE_URL}/api/v1/filaments", json=data)
    assert response.status_code == 201
    
    filament = response.json()
    assert filament["type"] == "PLA"
    assert filament["color"] == "Blue"
    assert filament["total_weight_in_grams"] == 1000
    assert filament["remaining_weight_in_grams"] == 1000
    assert "id" in filament

def test_create_print_job():
    """Test creating a print job."""
    # First get a printer and filament ID
    printers = requests.get(f"{BASE_URL}/api/v1/printers").json()
    filaments = requests.get(f"{BASE_URL}/api/v1/filaments").json()
    
    if not printers or not filaments:
        pytest.skip("No printers or filaments available for testing")
    
    printer_id = printers[0]["id"]
    filament_id = filaments[0]["id"]
    
    data = {
        "printer_id": printer_id,
        "filament_id": filament_id,
        "filepath": "tests/sample.gcode",
        "print_weight_in_grams": 100
    }
    
    response = requests.post(f"{BASE_URL}/api/v1/print_jobs", json=data)
    assert response.status_code == 201
    
    job = response.json()
    assert job["printer_id"] == printer_id
    assert job["filament_id"] == filament_id
    assert job["filepath"] == "tests/sample.gcode"
    assert job["print_weight_in_grams"] == 100
    assert job["status"] == "Queued"

def test_update_print_job_status():
    """Test updating a print job status."""
    # First create a print job
    jobs = requests.get(f"{BASE_URL}/api/v1/print_jobs").json()
    
    if not jobs:
        pytest.skip("No print jobs available for testing")
    
    job_id = jobs[0]["id"]
    
    # Update status to Running
    response = requests.post(f"{BASE_URL}/api/v1/print_jobs/{job_id}/status?status=Running")
    assert response.status_code == 200
    
    job = response.json()
    assert job["status"] == "Running"
    
    # Update status to Done
    response = requests.post(f"{BASE_URL}/api/v1/print_jobs/{job_id}/status?status=Done")
    assert response.status_code == 200
    
    job = response.json()
    assert job["status"] == "Done"