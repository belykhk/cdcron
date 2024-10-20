import atexit
import json
import logging
import os
import random
import string
import threading
import time

import requests

logger = logging.getLogger(__name__)


class Consul:

    def __init__(self):
        # Read env variables
        self.consul_scheme = os.getenv("CONSUL_SCHEME", "http")
        self.consul_hostname = os.getenv("CONSUL_HOSTNAME", "localhost")
        self.consul_port = int(os.getenv("CONSUL_PORT", 8500))
        self.consul_token = os.getenv("CONSUL_TOKEN", None)
        self.service_name = os.getenv("SERVICE_NAME", "cdcron")
        self.service_id = os.getenv(
            "SERVICE_ID",
            "".join(random.choices(string.ascii_uppercase + string.digits, k=5)),
        )
        self.healthcheck_scheme = os.getenv("HEALTHCHECK_SCHEME", "http")
        self.healthcheck_hostname = os.getenv(
            "HEALTHCHECK_HOSTNAME",
            "host.docker.internal",
        )
        self.healthcheck_port = int(os.getenv("HEALTHCHECK_PORT", 8080))

        self.is_leader = False
        self.start_consul()

    def _get_headers(self):
        """Function to simplify work with consul_token"""

        headers = {"Content-Type": "application/json"}
        if self.consul_token:
            headers["X-Consul-Token"] = self.consul_token
        return headers

    def start_consul(self):
        """Main thread of consul worker"""

        # Internal state machine
        self.registered = threading.Event()
        self.session_id = None
        self.election_key = f"service/{self.service_name}/leader"

        atexit.register(self.cleanup)
        self.deregister_service()

        # Registration
        registration_thread = threading.Thread(target=self.run_registration, daemon=True)
        registration_thread.start()
        while not self.registered.is_set():
            pass  # Wait until registration complete to continue

        # Session
        session_thread = threading.Thread(target=self.run_session_management, daemon=True)
        session_thread.start()
        while not self.session_id:
            pass  # Wait until we get session to continue

        # Election
        election_thread = threading.Thread(target=self.run_election, daemon=True)
        election_thread.start()

    def cleanup(self):
        """Handler of all exit code for clean shutdown"""
        logger.info("stopping consul client...")
        self.release_lock()
        self.deregister_service()

    def run_registration(self):
        """Thread to run registration and periodical checks if registration is still valid"""

        def _register(self):
            """Register the service with Consul including the health check."""

            url = f"{self.consul_scheme}://{self.consul_hostname}:{self.consul_port}/v1/agent/service/register"
            payload = {
                "ID": self.service_id,
                "Name": self.service_name,
                "Address": self.healthcheck_hostname,
                "Port": self.healthcheck_port,
                "Check": {
                    "HTTP": f"{self.healthcheck_scheme}://{self.healthcheck_hostname}:{self.healthcheck_port}",
                    "Interval": "5s",
                    "Timeout": "1s",
                    "DeregisterCriticalServiceAfter": "30s",
                },
            }
            headers = self._get_headers()

            try:
                response = requests.put(url, data=json.dumps(payload), headers=headers)
                if response.status_code == 200:
                    logger.info(f"service '{self.service_name}' with ID '{self.service_id}' registered successfully.")
                    self.registered.set()
                    return True
                else:
                    logger.error(
                        f"failed to register service '{self.service_name}', status code: {response.status_code}, message: {str(response.content)}"
                    )
                    os._exit(1)
            except Exception as e:
                logger.error(f"error registering service: {e}")
                os._exit(1)

        def _check_register(self):
            """Check if the service is registered and healthy."""

            url = f"{self.consul_scheme}://{self.consul_hostname}:{self.consul_port}/v1/catalog/service/{self.service_name}"
            headers = self._get_headers()

            try:
                response = requests.get(url, headers=headers)
                if response.status_code == 200:
                    data = response.json()
                    if not data:
                        logger.error(f"service registration lost, trying to reregister")
                        _register(self)
                    else:
                        logger.debug(f"service is still registered")
                        pass
                else:
                    logger.error(
                        f"failed to check service registration, status code: {response.status_code}, message: {str(response.content)}"
                    )
                    os._exit(1)
            except Exception as e:
                logger.error(f"error checking service registration: {e}")
                os._exit(1)

        if _register(self):
            while self.registered:
                time.sleep(10)
                _check_register(self)

    def run_session_management(self):
        """Thread to create and update consul session"""

        def _create_session(self):
            """Create consul session"""

            url = f"{self.consul_scheme}://{self.consul_hostname}:{self.consul_port}/v1/session/create"
            payload = {
                "Name": self.service_name,
                "TTL": "15s",
                "LockDelay": "0s",
                "Behavior": "delete",
            }
            headers = self._get_headers()

            try:
                response = requests.put(url, data=json.dumps(payload), headers=headers)
                if response.status_code == 200:
                    self.session_id = response.json().get("ID")
                    logger.info(f"session created with ID: '{self.session_id}'")
                    return True
                else:
                    logger.error(
                        f"failed to create session, status code: {response.status_code}, message: {str(response.content)}"
                    )
                    os._exit(1)
            except Exception as e:
                logger.error(f"error creating session: {e}")
                os._exit(1)

        def _renew_session(self):
            """Renews the session to maintain leadership."""

            url = f"{self.consul_scheme}://{self.consul_hostname}:{self.consul_port}/v1/session/renew/{self.session_id}"
            headers = self._get_headers()

            try:
                response = requests.put(url, headers=headers)
                if response.status_code == 200:
                    logger.debug(f"session {self.session_id} renewed.")
                    return True
                else:
                    logger.error(
                        f"failed to renew session, status code: {response.status_code}, message: {str(response.content)}"
                    )
                    os._exit(1)
            except Exception as e:
                logger.error(f"error renewing session: {e}")
                os._exit(1)

        if _create_session(self):
            while True:
                time.sleep(5)
                _renew_session(self)

    def run_election(self):
        """Thread to run consul election"""

        def _acquire_lock(self):
            """Acquire lock with current session ID to became leader"""
            url = f"{self.consul_scheme}://{self.consul_hostname}:{self.consul_port}/v1/kv/{self.election_key}?acquire={self.session_id}"
            headers = self._get_headers()
            payload = {"leader": self.session_id}

            try:
                response = requests.put(url, headers=headers, data=json.dumps(payload))
                if response.status_code == 200 and response.json():
                    logger.info("lock acquired, this instance is now the leader.")
                    self.is_leader = True
                    return True
                else:
                    logger.debug("failed to acquire lock, another instance may be the leader.")
                    return False
            except Exception as e:
                logger.error(f"error acquiring lock: {e}")
                os.exit(1)

        def _update_lock(self):
            """Update modify index of lock to indicate that leader is alive"""

            url = f"{self.consul_scheme}://{self.consul_hostname}:{self.consul_port}/v1/kv/{self.election_key}?index={self._lock_modify_index}"
            headers = self._get_headers()

            try:
                response = requests.get(url, headers=headers)
                if response.status_code == 200 and response.json():
                    data = response.json()
                    self._lock_modify_index = data[0].get("ModifyIndex")
                    logger.debug(f"lock updated, new index is {self._lock_modify_index}")
                else:
                    logger.error(f"failed to update lock, status code: {response.status_code}")
            except Exception as e:
                logger.error(f"error updating lock: {e}")

        def _watch_lock(self):
            """Watch the lock key for changes to respond to leader election."""
            url = f"{self.consul_scheme}://{self.consul_hostname}:{self.consul_port}/v1/kv/{self.election_key}?wait=5s"
            headers = self._get_headers()

            try:
                response = requests.get(url, headers=headers)
                if response.status_code == 404:
                    _acquire_lock(self)
                elif response.status_code == 200:
                    data = response.json()
                    if data and data[0].get("Session") == self.session_id:
                        _update_lock(self)
                        logger.debug("still the leader.")
                    else:
                        if data[0].get("ModifyIndex") == self._lock_modify_index:
                            logger.info(f"'ModifyIndex' is not updating, trying to get leadership")
                            _acquire_lock(self)
                        else:
                            self._lock_modify_index = data[0].get("ModifyIndex")
                            logger.debug("not a leader")
                else:
                    logger.error(f"failed to watch lock, status code: {response.status_code}")
            except Exception as e:
                logger.error(f"error watching lock: {e}")

        self._lock_modify_index = int()
        while True:
            if self.is_leader:
                time.sleep(5)
            else:
                time.sleep(10)
            _watch_lock(self)

    def deregister_service(self):
        """Deregister service with Consul"""

        url = f"{self.consul_scheme}://{self.consul_hostname}:{self.consul_port}/v1/agent/service/deregister/{self.service_id}"
        headers = self._get_headers()

        try:
            response = requests.put(url, headers=headers)
            if response.status_code == 200:
                logger.info(f"service with ID '{self.service_id}' deregistered successfully.")
                self.registered.clear()
            elif response.status_code == 404:
                logger.debug(f"service wasn't registered")
                self.registered.clear()
            else:
                logger.error(
                    f"failed to deregister service with ID '{self.service_id}'. Status code: {response.status_code}"
                )
                os._exit(1)
        except Exception as e:
            logger.error(f"error deregistering service: {e}")
            os._exit(1)

    def release_lock(self):
        """Releases the lock if this instance holds it."""

        if not self.session_id:
            logger.error("no session created, cannot release lock.")
            return

        url = f"{self.consul_scheme}://{self.consul_hostname}:{self.consul_port}/v1/kv/{self.election_key}?release={self.session_id}"
        headers = self._get_headers()
        payload = {"leader": self.session_id}

        try:
            response = requests.put(url, headers=headers, data=json.dumps(payload))
            if response.status_code == 200:
                if response.content == b"true":
                    logger.info("lock released successfully.")
                else:
                    logger.info("no lock to release")
            else:
                logger.error(f"failed to release lock. Status code: {response.status_code}")
                os._exit(1)
        except Exception as e:
            logger.error(f"error releasing lock: {e}")
            os._exit(1)
