package armadactl

import (
	"fmt"
	"strings"

	"github.com/pkg/errors"

	"github.com/armadaproject/armada/internal/common"
	armadaslices "github.com/armadaproject/armada/internal/common/slices"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/client"
)

// Preempt one or more jobs.
func (a *App) Preempt(queue string, jobSetId string, jobIds []string, reason string) (outerErr error) {
	apiConnectionDetails := a.Params.ApiConnectionDetails

	fmt.Fprintf(a.Out, "Requesting preemption of %d job(s) matching queue: %s, job set: %s\n", len(jobIds), queue, jobSetId)
	return client.WithSubmitClient(apiConnectionDetails, func(c api.SubmitClient) error {
		ctx, cancel := common.ContextWithDefaultTimeout()
		defer cancel()

		_, err := c.PreemptJobs(ctx, &api.JobPreemptRequest{
			JobIds:   jobIds,
			JobSetId: jobSetId,
			Queue:    queue,
			Reason:   reason,
		})
		if err != nil {
			return errors.Wrapf(err, "error preempting jobs matching queue: %s, job set: %s", queue, jobSetId)
		}

		for _, jobId := range jobIds {
			fmt.Fprintf(a.Out, "Requested preemption for job %s\n", jobId)
		}
		return nil
	})
}

func (a *App) PreemptOnExecutor(executor string, queues []string, priorityClasses []string, pools []string) error {
	queueMsg := strings.Join(queues, ",")
	priorityClassesMsg := strings.Join(priorityClasses, ",")
	poolsMsg := strings.Join(pools, ",")
	// If the provided slice of queues is empty, jobs on all queues will be preempted
	if len(queues) == 0 {
		apiQueues, err := a.getAllQueuesAsAPIQueue(&QueueQueryArgs{})
		if err != nil {
			return fmt.Errorf("error preempting jobs on executor %s: %s", executor, err)
		}
		queues = armadaslices.Map(apiQueues, func(q *api.Queue) string { return q.Name })
		queueMsg = "all"
	}
	if len(pools) == 0 {
		poolsMsg = "all"
	}

	fmt.Fprintf(a.Out, "Requesting preemption of jobs matching executor: %s, queues: %s, priority-classes: %s, pools: %s\n", executor, queueMsg, priorityClassesMsg, poolsMsg)
	if err := a.Params.ExecutorAPI.PreemptOnExecutor(executor, queues, priorityClasses, pools); err != nil {
		return fmt.Errorf("error preempting jobs on executor %s: %s", executor, err)
	}
	return nil
}

func (a *App) PreemptOnNode(node string, executor string, queues []string, priorityClasses []string) error {
	queueMsg := strings.Join(queues, ",")
	priorityClassesMsg := strings.Join(priorityClasses, ",")
	// If the provided slice of queues is empty, jobs on all queues will be preempted
	if len(queues) == 0 {
		apiQueues, err := a.getAllQueuesAsAPIQueue(&QueueQueryArgs{})
		if err != nil {
			return fmt.Errorf("error preempting jobs on node %s: %s", node, err)
		}
		queues = armadaslices.Map(apiQueues, func(q *api.Queue) string { return q.Name })
		queueMsg = "all"
	}

	fmt.Fprintf(a.Out, "Requesting preemption of jobs matching node: %s, executor: %s, queues: %s, priority-classes: %s\n", node, executor, queueMsg, priorityClassesMsg)
	if err := a.Params.NodeAPI.PreemptOnNode(node, executor, queues, priorityClasses); err != nil {
		return fmt.Errorf("error preempting jobs on node %s: %s", node, err)
	}
	return nil
}

// PreemptOnQueues preempts all jobs on queues matching the provided QueueQueryArgs filter
func (a *App) PreemptOnQueues(args *QueueQueryArgs, priorityClasses []string, pools []string, dryRun bool) error {
	queues, err := a.getAllQueuesAsAPIQueue(args)
	if err != nil {
		return errors.Errorf("error fetching queues: %s", err)
	}

	priorityClassesMsg := strings.Join(priorityClasses, ",")
	poolsMsg := strings.Join(pools, ",")
	if len(pools) == 0 {
		poolsMsg = "all"
	}

	for _, queue := range queues {
		fmt.Fprintf(a.Out, "Requesting preemption of jobs matching queue: %s, priorityClasses: %s, pools: %s\n", queue.Name, priorityClassesMsg, poolsMsg)
		if !dryRun {
			if err := a.Params.QueueAPI.Preempt(queue.Name, priorityClasses, pools); err != nil {
				return fmt.Errorf("error preempting jobs on queue %s: %s", queue.Name, err)
			}
		}
	}
	return nil
}
