# 🖨️ Raft3D

Raft3D is a simple distributed system for managing 3D printers, filaments, and print jobs.  
It uses the Raft consensus algorithm for leader election, log replication, and fault tolerance.

## Features

- **Leader Election:** Raft selects one node as the leader.
- **Event Log:** All changes are recorded in a log.
- **Snapshotting:** Periodic snapshots help recover the system.
- **HTTP REST API:** Manage printers, filaments, and print jobs.

## Data Models

- **Printers:**  
  Unique ID, company (e.g., Creality), and model (e.g., Ender 3).

- **Filaments:**  
  Unique ID, type (PLA, PETG, ABS, TPU), color, total weight, and remaining weight.

- **Print Jobs:**  
  Unique ID, printer_id, filament_id, file path, print weight, and status (Queued, Running, Done, Canceled).  
  When a print job is Done, the filament’s remaining weight is reduced.

## API Endpoints

- **GET /healthcheck** - Health Check

- **POST /api/v1/printers** – Create a printer  
- **GET /api/v1/printers** – List printers

- **POST /api/v1/filaments** – Create a filament  
- **GET /api/v1/filaments** – List filaments

- **POST /api/v1/print_jobs** – Create a print job  
- **GET /api/v1/print_jobs** – List print jobs

- **POST /api/v1/print_jobs/<job_id>/status?status=new_status** – Update print job status

## Setup and Run

1. **Clone the repository**  
   `git clone <repo_url> && cd raft3d`

2. **Create a virtual environment**  

3. **Activate the virtual environment**

4. **Install dependencies**  
`pip install -r requirements.txt`

5. **Run the nodes (in separate terminals):**

`python run_node.py --node-id node1 --raft-port 5001 --api-port 8001 --cluster localhost:5001,localhost:5002,localhost:5003`

`python run_node.py --node-id node2 --raft-port 5002 --api-port 8002 --cluster localhost:5001,localhost:5002,localhost:5003`

`python run_node.py --node-id node3 --raft-port 5003 --api-port 8003 --cluster localhost:5001,localhost:5002,localhost:5003`

