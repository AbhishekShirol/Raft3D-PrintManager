from flask import Blueprint, jsonify, request
import logging

logger = logging.getLogger(__name__)

printer_id_counter = 1

def register_printer_endpoints(app, raft_node):
    """Register printer endpoints with the Flask app."""
    
    @app.route('/api/v1/printers', methods=['POST'])
    def create_printer():
        """Create a new printer."""
        try:
            global printer_id_counter  # reference global counter
            data = request.json
            if not data:
                return jsonify({'error': 'No data provided'}), 400
            
            # Validate input
            if not data.get('company') or not data.get('model'):
                return jsonify({'error': 'Missing required fields: company, model'}), 400
            
            printer_id = data.get("id")

            if printer_id is None:
                printer_id = printer_id_counter
                printer_id_counter += 1
            
            # Create command
            command = {
                'type': 'create_printer',
                'payload': {
                    'id': str(printer_id),
                    'company': data.get('company'),
                    'model': data.get('model')
                }
            }
            
            # Propose command to Raft cluster
            result = raft_node.propose_command(command)
            
            if result.get('success'):
                # Get state machine state to retrieve the created printer
                state = raft_node.get_state_machine_state()
                for printer in state['printers'].values():
                    if printer.get('company') == data.get('company') and printer.get('model') == data.get('model'):
                        return jsonify(printer), 201
                
                return jsonify({'error': 'Printer created but not found in state'}), 500
            else:
                error = result.get('error', 'Unknown error')
                if error == 'Not leader':
                    return jsonify({'error': 'Not the leader node', 'leader': result.get('leader')}), 307
                return jsonify({'error': error}), 500
        
        except Exception as e:
            logger.error(f"Error creating printer: {e}")
            return jsonify({'error': str(e)}), 500
    
    @app.route('/api/v1/printers', methods=['GET'])
    def list_printers():
        """List all available printers."""
        try:
            # Get state from the state machine
            state = raft_node.get_state_machine_state()
            printers = list(state['printers'].values())
            
            return jsonify(printers), 200
        
        except Exception as e:
            logger.error(f"Error listing printers: {e}")
            return jsonify({'error': str(e)}), 500