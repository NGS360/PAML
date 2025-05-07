#!/usr/bin/env python3
'''
Example script demonstrating how to use the WES Platform implementation
'''
import os
import sys
import time
import logging
import argparse

# Add the src directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.cwl_platform import PlatformFactory

def main():
    '''
    Main function
    '''
    # Set up logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)

    # Parse command line arguments
    parser = argparse.ArgumentParser(description='WES Platform Example')
    parser.add_argument('--api-endpoint', required=True, help='WES API endpoint URL')
    parser.add_argument('--auth-token', help='Authentication token for the WES API')
    parser.add_argument('--workflow', required=True, help='Path to workflow file or URL')
    parser.add_argument('--params', required=True, help='Path to workflow parameters JSON file')
    args = parser.parse_args()

    # Initialize the platform factory
    factory = PlatformFactory()

    try:
        # Get the WES platform
        platform = factory.get_platform('WES')
        logger.info("Initialized WES platform")

        # Connect to the WES API
        connected = platform.connect(
            api_endpoint=args.api_endpoint,
            auth_token=args.auth_token
        )
        
        if not connected:
            logger.error("Failed to connect to WES API")
            return 1

        logger.info("Connected to WES API")

        # Create a virtual project (not used by WES but required by the API)
        project = platform.create_project('wes-example', 'WES Example Project')
        logger.info("Created virtual project: %s", project['name'])

        # Load workflow parameters
        with open(args.params, 'r') as f:
            import json
            parameters = json.load(f)

        # Submit the workflow
        task = platform.submit_task(
            name="WES Example Task",
            project=project,
            workflow=args.workflow,
            parameters=parameters
        )

        if not task:
            logger.error("Failed to submit workflow")
            return 1

        logger.info("Submitted workflow with run ID: %s", task.run_id)

        # Monitor the workflow execution
        while True:
            state = platform.get_task_state(task, refresh=True)
            logger.info("Workflow state: %s", state)

            if state in ['Complete', 'Failed', 'Cancelled']:
                break

            time.sleep(10)  # Check every 10 seconds

        # Get the workflow outputs
        if state == 'Complete':
            outputs = platform.get_task_outputs(task)
            logger.info("Workflow completed successfully")
            logger.info("Outputs: %s", outputs)
            return 0
        else:
            logger.error("Workflow failed or was cancelled")
            return 1

    except Exception as e:
        logger.exception("Error: %s", e)
        return 1

if __name__ == '__main__':
    sys.exit(main())