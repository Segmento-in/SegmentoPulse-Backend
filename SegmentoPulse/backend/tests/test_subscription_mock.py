
import unittest
import sys
import os
from unittest.mock import MagicMock, patch

# Ensure app can be imported
sys.path.append(os.getcwd())

from fastapi.testclient import TestClient
from app.main import app

client = TestClient(app)

class TestSubscription(unittest.TestCase):
    
    @patch('app.routes.subscription.get_firebase_service')
    @patch('app.routes.subscription.get_brevo_service')
    def test_subscribe_valid_preference(self, mock_brevo, mock_firebase):
        # Setup mocks
        mock_firebase_instance = MagicMock()
        mock_firebase_instance.initialized = True
        mock_firebase_instance.add_subscriber.return_value = True
        mock_firebase.return_value = mock_firebase_instance
        
        mock_brevo_instance = MagicMock()
        mock_brevo.return_value = mock_brevo_instance

        payload = {
            "email": "test@example.com",
            "name": "Test User",
            "preference": "Morning"
        }
        response = client.post("/api/subscription/subscribe", json=payload)
        
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["status"], "success")
        
        # Verify call
        mock_firebase_instance.add_subscriber.assert_called_once()
        args = mock_firebase_instance.add_subscriber.call_args[0]
        self.assertEqual(args[0], "test@example.com")
        self.assertEqual(args[1]["preference"], "Morning")
        print("\n✅ test_subscribe_valid_preference PASSED")

    def test_invalid_preference(self):
        payload = {
            "email": "fail@example.com",
            "name": "Fail",
            "preference": "Invalid"
        }
        response = client.post("/api/subscription/subscribe", json=payload)
        self.assertEqual(response.status_code, 422)
        print("\n✅ test_invalid_preference PASSED")

if __name__ == '__main__':
    unittest.main()
