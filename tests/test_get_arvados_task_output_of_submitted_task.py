'''
This test case is to ensure that the task output is available when the task was submitted and monitored to completion.
This mimicks the behaviour of the launcher:
1. Submit workflow task
2. Wait for workflow to complete
3. Get workout output field

At present time, when the launcher waits for a workflow to complete, the launcher does not re-fetch the task and hence
does not have Arvados output uuid.
'''
