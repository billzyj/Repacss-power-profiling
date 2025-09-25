"""
Test fixtures for database setup and configuration
"""
import pytest
from unittest.mock import MagicMock, patch


@pytest.fixture
def test_ssh_config():
    """Test SSH configuration"""
    return {
        'hostname': 'test-ssh-host',
        'port': 22,
        'username': 'test-user',
        'private_key_path': '/test/ssh/key',
        'passphrase': None,
        'keepalive': 60
    }


@pytest.fixture
def test_database_config():
    """Test database configuration"""
    return {
        'host': 'test-db-host',
        'port': 5432,
        'database': 'test_db',
        'username': 'test_user',
        'password': 'test_password',
        'ssl_mode': 'prefer'
    }


@pytest.fixture
def mock_ssh_tunnel():
    """Mock SSH tunnel for testing"""
    with patch('src.database.client.subprocess.Popen') as mock_popen:
        mock_process = MagicMock()
        mock_process.poll.return_value = None  # Process is running
        mock_popen.return_value = mock_process
        yield mock_popen


@pytest.fixture
def mock_database_connection():
    """Mock database connection for testing"""
    with patch('src.database.client.psycopg2.connect') as mock_connect:
        mock_conn = MagicMock()
        mock_conn.info.host = 'localhost'
        mock_conn.info.port = 5432
        mock_conn.info.user = 'test_user'
        mock_conn.info.password = 'test_password'
        mock_conn.info.dbname = 'test_db'
        mock_connect.return_value = mock_conn
        yield mock_conn


@pytest.fixture
def test_database_manager():
    """Test database manager with mocked connections"""
    with patch('src.database.database.DatabaseManager') as mock_manager:
        mock_instance = MagicMock()
        mock_manager.return_value = mock_instance
        yield mock_instance


@pytest.fixture
def sample_database_schemas():
    """Sample database schemas for testing"""
    return {
        'h100': ['idrac', 'public'],
        'zen4': ['idrac', 'public'],
        'infra': ['irc', 'pdu', 'public']
    }


@pytest.fixture
def test_query_results():
    """Sample query results for testing"""
    return {
        'power_metrics': [
            {'metric_id': 'CPUPower', 'units': 'W'},
            {'metric_id': 'GPUPower', 'units': 'W'},
            {'metric_id': 'DRAMPwr', 'units': 'W'}
        ],
        'node_data': [
            {'hostname': 'rpg-93-1', 'node_type': 'compute'},
            {'hostname': 'rpg-93-2', 'node_type': 'compute'},
            {'hostname': 'irc-node-1', 'node_type': 'irc'}
        ]
    }


@pytest.fixture
def mock_connection_pool():
    """Mock connection pool for testing"""
    with patch('src.database.connection_pool.ConnectionPool') as mock_pool:
        mock_instance = MagicMock()
        mock_pool.return_value = mock_instance
        yield mock_instance
