# GA4GH WES Platform Implementation

This document describes the GA4GH Workflow Execution Service (WES) platform implementation for the PAML library.

## Overview

The GA4GH WES API provides a standard way to submit and manage workflows across different workflow execution systems. This implementation allows you to use the PAML library to submit workflows to any WES-compatible service.

The WES platform implementation (`WESPlatform`) inherits from the `Platform` abstract base class and implements all the required methods to interact with a WES API endpoint.

## Features

- Connect to any WES API endpoint
- Submit CWL workflows
- Monitor workflow execution status
- Retrieve workflow outputs

## Limitations

Since the WES API is focused on workflow execution and doesn't include concepts like projects, folders, or file management, some features of the Platform API are not fully supported:

- File operations (upload, download, copy) are limited
- Project management is simulated using virtual projects
- User management is not supported

## Usage

### Initialization and Connection

```python
from src.cwl_platform import PlatformFactory

# Initialize the platform factory
factory = PlatformFactory()

# Get the WES platform
platform = factory.get_platform('WES')

# Connect to the WES API
platform.connect(
    api_endpoint="https://wes.example.com/ga4gh/wes/v1",
    auth_token="your_auth_token"  # Optional
)
```

### Submitting a Workflow

```python
# Create a virtual project (not used by WES but required by the API)
project = platform.create_project('wes-example', 'WES Example Project')

# Define workflow parameters
parameters = {
    "input_file": {
        "class": "File",
        "path": "https://example.com/input.txt"
    },
    "output_filename": "output.txt"
}

# Submit the workflow
task = platform.submit_task(
    name="My Workflow",
    project=project,
    workflow="https://example.com/workflow.cwl",  # URL or local file path
    parameters=parameters
)

# Get the run ID
run_id = task.run_id
```

### Monitoring Workflow Execution

```python
# Check the workflow state
state = platform.get_task_state(task)
print(f"Workflow state: {state}")

# Refresh the state from the server
state = platform.get_task_state(task, refresh=True)
print(f"Updated workflow state: {state}")

# Monitor until completion
import time
while True:
    state = platform.get_task_state(task, refresh=True)
    print(f"Workflow state: {state}")
    
    if state in ['Complete', 'Failed', 'Cancelled']:
        break
        
    time.sleep(10)  # Check every 10 seconds
```

### Retrieving Workflow Outputs

```python
# Get all outputs
outputs = platform.get_task_outputs(task)
print(f"Outputs: {outputs}")

# Get a specific output
output_file = platform.get_task_output(task, "output_file")
print(f"Output file: {output_file}")
```

## Example Script

See the `examples/wes_platform_example.py` script for a complete example of using the WES platform implementation.

## Configuration

The WES platform can be configured using the following environment variables:

- `WES_API_ENDPOINT`: The URL of the WES API endpoint
- `WES_AUTH_TOKEN`: Authentication token for the WES API

## Supported WES Implementations

This implementation should work with any WES-compatible service, including:

- [Cromwell](https://github.com/broadinstitute/cromwell)
- [Toil](https://github.com/DataBiosphere/toil)
- [WES-ELIXIR](https://github.com/elixir-cloud-aai/cwl-WES)
- [TESK](https://github.com/EMBL-EBI-TSI/TESK)
- [DNAstack WES](https://docs.dnastack.com/docs/workflow-execution-service-wes)

## References

- [GA4GH WES API Specification](https://github.com/ga4gh/workflow-execution-service-schemas)
- [WES API Documentation](https://ga4gh.github.io/workflow-execution-service-schemas/)