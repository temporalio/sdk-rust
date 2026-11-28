When shutdown and a freed task slot are both ready, the Worker stops polling.
Sticky polls, normal polls and eager starts share the Workflow cache slot limit.
Once Workflow polling starts, shutdown drains queued eager tasks and in-flight polls.
Large Workflow cache limits no longer allocate empty entries for the full capacity.
