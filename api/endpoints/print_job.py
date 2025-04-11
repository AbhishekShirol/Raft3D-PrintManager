from flask import Blueprint, jsonify, request
import logging
import time

logger = logging.getLogger(__name__)

def register_print_job_endpoints(app, raft_node):
    """Register print job endpoints with the Flask app."""
    
    @app.route('/api/v1/print_jobs', methods=['POST'])
    def create_print_job():
        """Create a new print job."""
        try:
            data = request.json
            if not data:
                return jsonify({'error': 'No data provided'}), 400
            
            # Validate input
            required_fields = ['printer_id', 'filament_id', 'filepath', 'print_weight_in_grams']
            for field in required_fields:
                if not data.get(field):
                    return jsonify({'error': f'Missing required field: {field}'}), 400
            
            # Create command
            command = {
                'type': 'create_print_job',
                'payload': {
                    'id': data.get('id'),
                    'printer_id': data.get('printer_id'),
                    'filament_id': data.get('filament_id'),
                    'filepath': data.get('filepath'),
                    'print_weight_in_grams': data.get('print_weight_in_grams')
                }
            }
            
            # Propose command to Raft cluster
            result = raft_node.propose_command(command)
            
            if result.get('success'):
                # Get state machine state to retrieve the created print job
                state = raft_node.get_state_machine_state()
                for job in state['print_jobs'].values():
                    if (job.get('printer_id') == data.get('printer_id') and 
                        job.get('filament_id') == data.get('filament_id') and
                        job.get('filepath') == data.get('filepath')):
                        return jsonify(job), 201
                
                return jsonify({'error': 'Print job created but not found in state'}), 500
            else:
                error = result.get('error', 'Unknown error')
                if error == 'Not leader':
                    return jsonify({'error': 'Not the leader node', 'leader': result.get('leader')}), 307
                return jsonify({'error': error}), 500
        
        except Exception as e:
            logger.error(f"Error creating print job: {e}")
            return jsonify({'error': str(e)}), 500
    
    @app.route('/api/v1/print_jobs', methods=['GET'])
    def list_print_jobs():
        """List all print jobs."""
        try:
            # Get state from the state machine
            state = raft_node.get_state_machine_state()
            jobs = list(state['print_jobs'].values())
            
            # Filter by status if provided
            status_filter = request.args.get('status')
            if status_filter:
                jobs = [job for job in jobs if job.get('status') == status_filter]
            
            return jsonify(jobs), 200
        
        except Exception as e:
            logger.error(f"Error listing print jobs: {e}")
            return jsonify({'error': str(e)}), 500

    # @app.route('/api/v1/print_jobs', methods=['POST'])
    # def create_print_job():
    #     """Create a new print job."""
    #     try:
    #         data = request.json
    #         if not data:
    #             return jsonify({'error': 'No data provided'}), 400
            
    #         # Validate input
    #         required_fields = ['printer_id', 'filament_id', 'filepath', 'print_weight_in_grams']
    #         for field in required_fields:
    #             if not data.get(field):
    #                 return jsonify({'error': f'Missing required field: {field}'}), 400
            
    #         # Create command
    #         command = {
    #             'type': 'create_print_job',
    #             'payload': {
    #                 # Use the provided id; if absent, the state machine will generate one.
    #                 'id': data.get('id'),
    #                 'printer_id': data.get('printer_id'),
    #                 'filament_id': data.get('filament_id'),
    #                 'filepath': data.get('filepath'),
    #                 'print_weight_in_grams': data.get('print_weight_in_grams')
    #             }
    #         }
            
    #         # Propose command to Raft cluster
    #         result = raft_node.propose_command(command)
            
    #         if result.get('success'):
    #             # Wait up to 5 seconds for the state machine to reflect the new print job
    #             timeout = 5.0
    #             start_time = time.time()
    #             job_id = data.get('id')
    #             # If no job id was provided, we cannot easily match the entry.
    #             if not job_id:
    #                 # Optionally, you could require an id in the payload
    #                 job_id = None
                
    #             while time.time() - start_time < timeout:
    #                 state = raft_node.get_state_machine_state()
    #                 if job_id is not None:
    #                     # Directly check for the job id
    #                     if job_id in state['print_jobs']:
    #                         return jsonify(state['print_jobs'][job_id]), 201
    #                 else:
    #                     # Fallback: match on the combination if id is not provided
    #                     for job in state['print_jobs'].values():
    #                         if (job.get('printer_id') == data.get('printer_id') and 
    #                             job.get('filament_id') == data.get('filament_id') and
    #                             job.get('filepath') == data.get('filepath')):
    #                             return jsonify(job), 201
    #                 time.sleep(0.1)
    #             return jsonify({'error': 'Print job created but not found in state'}), 500
    #         else:
    #             error = result.get('error', 'Unknown error')
    #             if error == 'Not leader':
    #                 return jsonify({'error': 'Not the leader node', 'leader': result.get('leader')}), 307
    #             return jsonify({'error': error}), 500

    #     except Exception as e:
    #         logger.error(f"Error creating print job: {e}")
    #         return jsonify({'error': str(e)}), 500

    
    @app.route('/api/v1/print_jobs/<job_id>/status', methods=['POST'])
    def update_print_job_status(job_id):
        """Update the status of a print job."""
        try:
            new_status = request.args.get('status')
            if not new_status:
                return jsonify({'error': 'Missing status parameter'}), 400
            
            # Create command
            command = {
                'type': 'update_print_job_status',
                'payload': {
                    'id': job_id,
                    'status': new_status
                }
            }
            
            # Propose command to Raft cluster
            result = raft_node.propose_command(command)
            
            if result.get('success'):
                # Get state machine state to retrieve the updated print job
                state = raft_node.get_state_machine_state()
                if job_id in state['print_jobs']:
                    return jsonify(state['print_jobs'][job_id]), 200
                
                return jsonify({'error': 'Print job not found'}), 404
            else:
                error = result.get('error', 'Unknown error')
                if error == 'Not leader':
                    return jsonify({'error': 'Not the leader node', 'leader': result.get('leader')}), 307
                return jsonify({'error': error}), 500
        
        except Exception as e:
            logger.error(f"Error updating print job status: {e}")
            return jsonify({'error': str(e)}), 500