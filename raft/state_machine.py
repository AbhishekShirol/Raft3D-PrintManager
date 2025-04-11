import json
import logging
import uuid

logger = logging.getLogger("StateMachine")

class RaftStateMachine:
    def __init__(self):
        # Initialize the finite state machine with empty state.
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
        """Create a new printer entry."""
        printer_id = payload.get('id', str(uuid.uuid4()))
        company = payload.get('company')
        model = payload.get('model')
        if not company or not model:
            return {'success': False, 'error': 'Missing required fields'}
        printer = {
            'id': printer_id,
            'company': company,
            'model': model
        }
        self.printers[printer_id] = printer
        logger.info(f"Created printer: {printer}")
        return {'success': True, 'printer': printer}

    def _create_filament(self, payload):
        """Create a new filament entry."""
        filament_id = payload.get('id', str(uuid.uuid4()))
        filament_type = payload.get('type')
        color = payload.get('color')
        total_weight = payload.get('total_weight_in_grams')
        if total_weight is None:
            return {'success': False, 'error': 'Missing total_weight_in_grams'}
        try:
            total_weight = int(total_weight)
        except ValueError:
            return {'success': False, 'error': 'Invalid total_weight_in_grams'}
        remaining_weight = payload.get('remaining_weight_in_grams', total_weight)
        try:
            remaining_weight = int(remaining_weight)
        except ValueError:
            remaining_weight = total_weight
        if not filament_type or not color or not total_weight:
            return {'success': False, 'error': 'Missing required fields'}
        valid_types = ['PLA', 'PETG', 'ABS', 'TPU']
        if filament_type not in valid_types:
            return {'success': False, 'error': f"Invalid filament type. Must be one of: {valid_types}"}
        filament = {
            'id': filament_id,
            'type': filament_type,
            'color': color,
            'total_weight_in_grams': total_weight,
            'remaining_weight_in_grams': remaining_weight
        }
        self.filaments[filament_id] = filament
        logger.info(f"Created filament: {filament}")
        return {'success': True, 'filament': filament}

    def _create_print_job(self, payload):
        """Create a new print job entry."""
        # Debug: log incoming payload
        logger.debug(f"_create_print_job payload: {payload}")
        job_id = payload.get('id', str(uuid.uuid4()))
        printer_id = payload.get('printer_id')
        filament_id = payload.get('filament_id')
        filepath = payload.get('filepath')
        print_weight = payload.get('print_weight_in_grams')
        if not printer_id or not filament_id or not filepath or not print_weight:
            return {'success': False, 'error': 'Missing required fields in print job'}
        if printer_id not in self.printers:
            return {'success': False, 'error': f"Printer with ID {printer_id} not found"}
        if filament_id not in self.filaments:
            return {'success': False, 'error': f"Filament with ID {filament_id} not found"}
        try:
            required_weight = int(print_weight)
        except ValueError:
            return {'success': False, 'error': 'Invalid print_weight_in_grams'}
        filament = self.filaments[filament_id]
        # Calculate used weight from queued or running jobs
        used_weight = sum(job['print_weight_in_grams'] for job in self.print_jobs.values() 
                          if job['filament_id'] == filament_id and job['status'] in ['Queued', 'Running'])
        available_weight = filament['remaining_weight_in_grams'] - used_weight
        logger.debug(f"Used weight: {used_weight}, Available weight: {filament['remaining_weight_in_grams']}, Required: {required_weight}")
        if available_weight < required_weight:
            return {'success': False, 'error': 'Not enough filament remaining'}
        print_job = {
            'id': job_id,
            'printer_id': printer_id,
            'filament_id': filament_id,
            'filepath': filepath,
            'print_weight_in_grams': required_weight,
            'status': 'Queued'
        }
        self.print_jobs[job_id] = print_job
        logger.info(f"Created print job: {print_job}")
        return {'success': True, 'print_job': print_job}

    # def _update_print_job_status(self, payload):
    #     """Update the status of an existing print job."""
    #     job_id = payload.get('id')
    #     new_status = payload.get('status')
    #     if not job_id or not new_status:
    #         return {'success': False, 'error': 'Missing required fields for updating status'}
    #     if job_id not in self.print_jobs:
    #         return {'success': False, 'error': f"Print job with ID {job_id} not found"}
    #     job = self.print_jobs[job_id]
    #     current_status = job['status']
    #     valid_transitions = {
    #         'Queued': ['Running', 'Canceled'],
    #         'Running': ['Done', 'Canceled'],
    #         'Done': [],
    #         'Canceled': []
    #     }
    #     if new_status not in valid_transitions.get(current_status, []):
    #         return {'success': False, 'error': f"Invalid status transition from {current_status} to {new_status}"}
    #     job['status'] = new_status
    #     # If the job is done, update filament remaining weight.
    #     if new_status == 'Done':
    #         filament_id = job['filament_id']
    #         filament = self.filaments[filament_id]
    #         filament['remaining_weight_in_grams'] -= job['print_weight_in_grams']
    #     logger.info(f"Updated print job {job_id} to status {new_status}")
    #     return {'success': True, 'print_job': job}

    def _update_print_job_status(self, payload):
        """Update the status of an existing print job."""
        job_id = payload.get('id')
        new_status = payload.get('status')
        if not job_id or not new_status:
            return {'success': False, 'error': 'Missing required fields for updating status'}
        if job_id not in self.print_jobs:
            return {'success': False, 'error': f"Print job with ID {job_id} not found"}
        job = self.print_jobs[job_id]
        current_status = job['status']
        valid_transitions = {
            'Queued': ['Running', 'Canceled'],
            'Running': ['Done', 'Canceled'],
            'Done': [],
            'Canceled': []
        }
        # if new_status not in valid_transitions.get(current_status, []):
        #     logger.error(f"Invalid status transition attempted from {current_status} to {new_status} for job {job_id}")
        #     return {'success': False, 'error': f"Invalid status transition from {current_status} to {new_status}"}
        if new_status not in valid_transitions.get(current_status, []):
            logger.warning(f"Invalid status transition from {current_status} to {new_status} for job {job_id}")
            return {'success': False, 'error': f"Invalid status transition from {current_status} to {new_status}"}

        job['status'] = new_status
        # If the job is done, update filament remaining weight.
        if new_status == 'Done':
            filament_id = job['filament_id']
            filament = self.filaments[filament_id]
            filament['remaining_weight_in_grams'] -= job['print_weight_in_grams']
        logger.info(f"Updated print job {job_id} to status {new_status}")
        return {'success': True, 'print_job': job}


    def get_state(self):
        """Return the current state of the FSM."""
        return {
            'printers': self.printers,
            'filaments': self.filaments,
            'print_jobs': self.print_jobs
        }
