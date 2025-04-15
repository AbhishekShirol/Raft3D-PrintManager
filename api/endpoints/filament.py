import uuid
from flask import Blueprint, jsonify, request
import logging


logger = logging.getLogger(__name__)

# 👇 Global counter (declared at the module level)
filament_id_counter = 1

def register_filament_endpoints(app, raft_node):
    """Register filament endpoints with the Flask app."""
    
    @app.route('/api/v1/filaments', methods=['POST'])
    def create_filament():
        """Create a new filament."""
        try:
            global filament_id_counter  # reference global counter
            data = request.json
            if not data:
                return jsonify({'error': 'No data provided'}), 400
            
            filament_id = data.get('id') or str(uuid.uuid4())
            
            # Validate input
            if not data.get('type') or not data.get('color') or not data.get('total_weight_in_grams'):
                return jsonify({'error': 'Missing required fields: type, color, total_weight_in_grams'}), 400
            
            # Validate filament type
            valid_types = ['PLA', 'PETG', 'ABS', 'TPU']
            if data.get('type') not in valid_types:
                return jsonify({'error': f'Invalid filament type. Must be one of: {valid_types}'}), 400
            
            # Set remaining weight to total weight if not provided
            remaining_weight = data.get('remaining_weight_in_grams', data.get('total_weight_in_grams'))

            # Use provided ID or auto-generate a simple one
            filament_id = data.get('id')
            if filament_id is None:
                filament_id = filament_id_counter
                filament_id_counter += 1
            
            # Create command
            command = {
                'type': 'create_filament',
                'payload': {
                    'id': str(filament_id),
                    'type': data.get('type'),
                    'color': data.get('color'),
                    'total_weight_in_grams': data.get('total_weight_in_grams'),
                    'remaining_weight_in_grams': remaining_weight
                }
            }
            
            # Propose command to Raft cluster
            result = raft_node.propose_command(command)
            
            if result.get('success'):
                # Get state machine state to retrieve the created filament
                state = raft_node.get_state_machine_state()
                for filament in state['filaments'].values():
                    if (filament.get('type') == data.get('type') and 
                        filament.get('color') == data.get('color') and
                        filament.get('total_weight_in_grams') == data.get('total_weight_in_grams')):
                        return jsonify(filament), 201
                
                return jsonify({'error': 'Filament created but not found in state'}), 500
            else:
                error = result.get('error', 'Unknown error')
                if error == 'Not leader':
                    return jsonify({'error': 'Not the leader node', 'leader': result.get('leader')}), 307
                return jsonify({'error': error}), 500
        
        except Exception as e:
            logger.error(f"Error creating filament: {e}")
            return jsonify({'error': str(e)}), 500
    
    @app.route('/api/v1/filaments', methods=['GET'])
    def list_filaments():
        """List all available filaments."""
        try:
            # Get state from the state machine
            state = raft_node.get_state_machine_state()
            filaments = list(state['filaments'].values())
            
            return jsonify(filaments), 200
        
        except Exception as e:
            logger.error(f"Error listing filaments: {e}")
            return jsonify({'error': str(e)}), 500