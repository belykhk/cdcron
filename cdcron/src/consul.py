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

        self.start_consul()

        # # Startup registration.
        # # Deregistration is needed to delete old instance in case we moved it
        # self.register_service()
        # atexit.register(self.cleanup)
        # self.create_session()

        # # Start the election and registration check threads
        # self.start_election()
        # self.start_registration_check()

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

        atexit.register(self.cleanup)
        self.deregister_service()

        # Registration
        registration_thread = threading.Thread(
            target=self.run_registration, daemon=True
        )
        registration_thread.start()
        while not self.registered.is_set():
            pass  # Wait until registration complete to continue

        # Session
        session_thread = threading.Thread(
            target=self.run_session_management, daemon=True
        )
        session_thread.start()
        while not self.session_id:
            pass  # Wait until we get session to continue

    def cleanup(self):
        """Handler of all exit code for clean shutdown"""
        logger.info("stopping consul client...")
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
                    logger.info(
                        f"service '{self.service_name}' with ID '{self.service_id}' registered successfully."
                    )
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
                        pass  # Serivce is still registered
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

    def deregister_service(self):
        """Deregister service with Consul"""

        url = f"{self.consul_scheme}://{self.consul_hostname}:{self.consul_port}/v1/agent/service/deregister/{self.service_id}"
        headers = self._get_headers()

        try:
            response = requests.put(url, headers=headers)
            if response.status_code == 200:
                logger.info(
                    f"service with ID '{self.service_id}' deregistered successfully."
                )
                self.registered.clear()
            elif response.status_code == 404:
                self.registered.clear()
                pass  # There is already no service, no error
            else:
                logger.error(
                    f"failed to deregister service with ID '{self.service_id}'. Status code: {response.status_code}"
                )
                os._exit(1)
        except Exception as e:
            logger.error(f"error deregistering service: {e}")
            os._exit(1)

    # def create_session(self):
    #     """Creates a Consul session for leader election."""

    # # Election
    # def start_election(self):
    #     """Start the election process in a non-blocking thread."""
    #     self.election_thread = threading.Thread(target=self.run_election, daemon=True)
    #     self.election_thread.start()

    #     # Start watching the lock in a separate thread
    #     watch_thread = threading.Thread(target=self.watch_lock, daemon=True)
    #     watch_thread.start()

    # def run_election(self):
    #     """Leader election logic that keeps trying to acquire/maintain leadership."""

    #     while not self.stop_election.is_set():
    #         if self.acquire_lock():
    #             # Only renew the session while we hold the lock
    #             while self.renew_session():
    #                 time.sleep(10)  # Sleep until the next renewal
    #                 if self.stop_election.is_set():
    #                     break
    #         else:
    #             time.sleep(5)

    # def watch_lock(self):
    #     """Watch the lock key for changes to respond to leader election."""
    #     url = f"{self.consul_scheme}://{self.consul_hostname}:{self.consul_port}/v1/kv/{self.election_key}?wait=5s"
    #     headers = self._get_headers()

    #     while not self.stop_election.is_set():
    #         try:
    #             response = requests.get(url, headers=headers)
    #             if response.status_code == 200:
    #                 data = response.json()
    #                 # Check if the lock is held by this instance
    #                 if data and data[0].get("Session") == self.session_id:
    #                     logger.debug("still the leader.")
    #                 else:
    #                     logger.info("lost leadership, attempting to acquire lock.")
    #                     self.acquire_lock()  # Attempt to reacquire leadership
    #             else:
    #                 logger.error(
    #                     f"failed to watch lock. Status code: {response.status_code}"
    #                 )
    #         except requests.exceptions.Timeout:
    #             logger.info("Watch request timed out. Trying again...")
    #         except Exception as e:
    #             logger.error(f"error watching lock: {e}")

    # def acquire_lock(self):
    #     """Attempts to acquire a lock to become the leader using the watch feature."""

    #     if not self.session_id:
    #         logger.error("no session created, cannot acquire lock.")
    #         return False

    #     url = f"{self.consul_scheme}://{self.consul_hostname}:{self.consul_port}/v1/kv/{self.election_key}?acquire={self.session_id}"
    #     headers = self._get_headers()

    #     try:
    #         response = requests.put(url, headers=headers)
    #         if response.status_code == 200 and response.json():
    #             logger.info("lock acquired, this instance is now the leader.")
    #             return True
    #         else:
    #             logger.debug(
    #                 "failed to acquire lock, another instance may be the leader."
    #             )
    #             return False
    #     except Exception as e:
    #         logger.error(f"error acquiring lock: {e}")
    #         return False

    # # Exit code
    # def cleanup(self):
    #     """Graceful cleanup on application exit."""

    #     self.stop_election_process()
    #     self.release_lock()
    #     self.deregister_service()

    # def stop_election_process(self):
    #     """Stop the election thread."""

    #     self.stop_election.set()
    #     if self.election_thread:
    #         self.election_thread.join()

    # def release_lock(self):
    #     """Releases the lock if this instance holds it."""

    #     if not self.session_id:
    #         logger.error("no session created, cannot release lock.")
    #         return

    #     url = f"{self.consul_scheme}://{self.consul_hostname}:{self.consul_port}/v1/kv/{self.election_key}?release={self.session_id}"
    #     headers = self._get_headers()

    #     try:
    #         response = requests.put(url, headers=headers)
    #         if response.status_code == 200:
    #             if str(response.content) == "true":
    #                 logger.info("lock released successfully.")
    #             else:
    #                 logger.info("no lock to release")
    #         else:
    #             logger.error(
    #                 f"failed to release lock. Status code: {response.status_code}"
    #             )
    #             os._exit(1)
    #     except Exception as e:
    #         logger.error(f"error releasing lock: {e}")
    #         os._exit(1)
