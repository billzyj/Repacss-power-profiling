"""
Integration tests for database connections and SSH tunnels
"""
import pytest
from unittest.mock import patch, MagicMock
from src.database.database import DatabaseManager
from src.database.client import REPACSSPowerClient


class TestDatabaseConnections:
    """Test database connection and SSH tunnel functionality"""
    
    @pytest.fixture
    def mock_ssh_config(self):
        """Mock SSH configuration for testing"""
        return {
            'hostname': 'test-ssh-host',
            'port': 22,
            'username': 'test-user',
            'private_key_path': '/test/key',
            'passphrase': None
        }
    
    @pytest.fixture
    def mock_db_config(self):
        """Mock database configuration for testing"""
        return {
            'host': 'test-db-host',
            'port': 5432,
            'database': 'test_db',
            'username': 'test_user',
            'password': 'test_password',
            'ssl_mode': 'prefer'
        }
    
    @patch('src.database.client.subprocess.Popen')
    def test_ssh_tunnel_creation(self, mock_popen, mock_ssh_config, mock_db_config):
        """Test SSH tunnel creation"""
        # Mock successful subprocess
        mock_process = MagicMock()
        mock_process.poll.return_value = None  # Process is running
        mock_popen.return_value = mock_process
        
        # Create client
        client = REPACSSPowerClient(mock_db_config, mock_ssh_config)
        
        # Test connection
        client.connect()
        
        # Verify SSH command was called correctly
        mock_popen.assert_called_once()
        ssh_cmd = mock_popen.call_args[0][0]
        assert 'ssh' in ssh_cmd
        assert '-N' in ssh_cmd
        assert '-L' in ssh_cmd
    
    @patch('src.database.client.psycopg2.connect')
    def test_database_connection(self, mock_connect, mock_ssh_config, mock_db_config):
        """Test database connection through SSH tunnel"""
        # Mock successful database connection
        mock_conn = MagicMock()
        mock_conn.info.host = 'localhost'
        mock_conn.info.port = 5432
        mock_conn.info.user = 'test_user'
        mock_conn.info.password = 'test_password'
        mock_conn.info.dbname = 'test_db'
        mock_connect.return_value = mock_conn
        
        # Mock SSH tunnel
        with patch('src.database.client.subprocess.Popen') as mock_popen:
            mock_process = MagicMock()
            mock_process.poll.return_value = None
            mock_popen.return_value = mock_process
            
            client = REPACSSPowerClient(mock_db_config, mock_ssh_config)
            client.connect()
            
            # Verify database connection parameters
            mock_connect.assert_called_once()
            call_args = mock_connect.call_args
            assert call_args[1]['host'] == 'localhost'
            assert call_args[1]['database'] == 'test_db'
    
    def test_connection_pooling(self):
        """Test database connection pooling"""
        db_manager = DatabaseManager()
        
        # Test multiple connections to same database
        client1 = db_manager.create_client_for_database('h100')
        client2 = db_manager.create_client_for_database('h100')
        
        # Should reuse connections when possible
        assert client1 is not None
        assert client2 is not None
    
    def test_connection_cleanup(self):
        """Test proper cleanup of connections and tunnels"""
        with patch('src.database.client.subprocess.Popen') as mock_popen:
            mock_process = MagicMock()
            mock_process.poll.return_value = None
            mock_popen.return_value = mock_process
            
            client = REPACSSPowerClient(mock_db_config, mock_ssh_config)
            client.connect()
            client.disconnect()
            
            # Verify cleanup was called
            mock_process.terminate.assert_called_once()
            mock_process.wait.assert_called_once()
