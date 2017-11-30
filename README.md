# scala-task-scheduler
This is a project that implements custom task scheduler in scala

massively based on akka.Actor.LightArrayRevolverScheduler, but queue logic is different:
	tasks are sorted by datetime, tasks with equal datetime are enqueued internally
	so there is no need to change all tasks "ticks left to run", 
	only first datetime values are checked to be lower or equal to "now"