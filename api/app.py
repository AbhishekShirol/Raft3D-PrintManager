from flask import Flask, jsonify, request
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_app(raft_node):
    """Create and configure the Flask application."""
    app = Flask(__name__)
    
    # Import endpoints
    from api.endpoints.printer import register_printer_endpoints
    from api.endpoints.filament import register_filament_endpoints
    from api.endpoints.print_job import register_print_job_endpoints
    
    # Register endpoints
    register_printer_endpoints(app, raft_node)
    register_filament_endpoints(app, raft_node)
    register_print_job_endpoints(app, raft_node)
    
    # Define error handlers
    @app.errorhandler(404)
    def not_found(error):
        return jsonify({'error': 'Not found'}), 404
    
    @app.errorhandler(400)
    def bad_request(error):
        return jsonify({'error': 'Bad request'}), 400
    
    @app.errorhandler(500)
    def internal_error(error):
        logger.error(f"Internal server error: {error}")
        return jsonify({'error': 'Internal server error'}), 500
    
    # Define healthcheck endpoint
    @app.route('/healthcheck', methods=['GET'])
    def healthcheck():
        node_status = "leader" if raft_node.state.name == "LEADER" else "follower"
        return jsonify({
            'status': 'ok',
            'node_id': raft_node.id,
            'node_state': node_status
        })
    
    
    @app.route("/")
    def home():
        return jsonify({"message": "Raft3D API is running"}), 200       
    
    return app