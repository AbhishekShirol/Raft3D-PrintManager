import json
import logging
import uuid

logger = logging.getLogger("StateMachine")

class RaftStateMachine:
    def __init__(self):
        # Initialize empty state
        self.printers = {}
        self.filaments = {}
        self.print_jobs = {}

    def apply(self, command):
        """Apply a command to the state machine."""
        try:
            cmd_type = command.get('type')
            payload = command.get('payload', {})
            
            if cmd_type == 'create_printer':
                return self._create_printer(payload)
            elif cmd_type == 'create_filament':
                return self._create_filament(payload)
            elif cmd_type == 'create_print_job':
                return self._create_print_job(payload)
            elif cmd_type == 'update_print_job_status':
                return self._update_print_job_status(payload)
            else:
                logger.warning(f"Unknown command type: {cmd_type}")
                return {'success': False, 'error': f"Unknown command type: {cmd_type}"}
                
        except Exception as e:
            logger.error(f"Error applying command: {e}")
            return {'success': False, 'error': str(e)}

    def _create_printer(self, payload):
        """Create a new printer."""
        printer_id = payload.get('id', str(uuid.uuid4()))
        company = payload.get('company')
        model = payload.get('model')
        
        # Basic validation
        if not company or not model:
            return {'success': False, 'error': 'Missing required fields'}
        
        # Create printer
        printer = {
            'id': printer_id,
            'company': company,
            'model': model
        }
        
        self.printers[printer_id] = printer
        return {'success': True, 'printer': printer}

    def _create_filament(self, payload):
        """Create a new filament."""
        filament_id = payload.get('id', str(uuid.uuid4()))
        filament_type = payload.get('type')
        color = payload.get('color')
        total_weight = payload.get('total_weight_in_grams')
        remaining_weight = payload.get('remaining_weight_in_grams', total_weight)
        
        # Basic validation
        if not filament_type or not color or not total_weight:
            return {'success': False, 'error': 'Missing required fields'}
            
        # Validate filament type
        valid_types = ['PLA', 'PETG', 'ABS', 'TPU']
        if filament_type not in valid_types:
            return {'success': False, 'error': f'Invalid filament type. Must be one of: {valid_types}'}
        
        # Create filament
        filament = {
            'id': filament_id,
            'type': filament_type,
            'color': color,
            'total_weight_in_grams': int(total_weight),
            'remaining_weight_in_grams': int(remaining_weight)
        }
        
        self.filaments[filament_id] = filament
        return {'success': True, 'filament': filament}

    def _create_print_job(self, payload):
        """Create a new print job."""
        job_id = payload.get('id', str(uuid.uuid4()))
        printer_id = payload.get('printer_id')
        filament_id = payload.get('filament_id')
        filepath = payload.get('filepath')
        print_weight = payload.get('print_weight_in_grams')
        
        # Basic validation
        if not printer_id or not filament_id or not filepath or not print_weight:
            return {'success': False, 'error': 'Missing required fields'}
        
        # Validate printer exists
        if printer_id not in self.printers:
            return {'success': False, 'error': f'Printer with ID {printer_id} not found'}
        
        # Validate filament exists
        if filament_id not in self.filaments:
            return {'success': False, 'error': f'Filament with ID {filament_id} not found'}
        
        # Check if filament has enough remaining weight
        filament = self.filaments[filament_id]
        required_weight = int(print_weight)
        
        # Calculate weight used by other queued/running jobs using this filament
        used_weight = 0
        for job in self.print_jobs.values():
            if job['filament_id'] == filament_id and job['status'] in ['Queued', 'Running']:
                used_weight += job['print_weight_in_grams']
        
        # Check if enough filament remains
        if filament['remaining_weight_in_grams'] - used_weight < required_weight:
            return {'success': False, 'error': 'Not enough filament remaining'}
        
        # Create print job
        print_job = {
            'id': job_id,
            'printer_id': printer_id,
            'filament_id': filament_id,
            'filepath': filepath,
            'print_weight_in_grams': required_weight,
            'status': 'Queued'
        }
        
        self.print_jobs[job_id] = print_job
        return {'success': True, 'print_job': print_job}

    def _update_print_job_status(self, payload):
        """Update the status of a print job."""
        job_id = payload.get('id')
        new_status = payload.get('status')
        
        # Basic validation
        if not job_id or not new_status:
            return {'success': False, 'error': 'Missing required fields'}
        
        # Validate job exists
        if job_id not in self.print_jobs:
            return {'success': False, 'error': f'Print job with ID {job_id} not found'}
        
        # Get current job
        job = self.print_jobs[job_id]
        current_status = job['status']
        
        # Validate status transition
        valid_transitions = {
            'Queued': ['Running', 'Canceled'],
            'Running': ['Done', 'Canceled'],
            'Done': [],
            'Canceled': []
        }
        
        if new_status not in valid_transitions.get(current_status, []):
            return {'success': False, 'error': f'Invalid status transition from {current_status} to {new_status}'}
        
        # Update status
        job['status'] = new_status
        
        # If status is Done, update filament remaining weight
        if new_status == 'Done':
            filament_id = job['filament_id']
            filament = self.filaments[filament_id]
            filament['remaining_weight_in_grams'] -= job['print_weight_in_grams']
        
        return {'success': True, 'print_job': job}

    def get_state(self):
        """Get the current state of the state machine."""
        return {
            'printers': self.printers,
            'filaments': self.filaments,
            'print_jobs': self.print_jobs
        }