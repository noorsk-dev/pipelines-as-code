package gitlab

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/openshift-pipelines/pipelines-as-code/pkg/kubeinteraction"
	"github.com/openshift-pipelines/pipelines-as-code/pkg/matcher"
	"github.com/openshift-pipelines/pipelines-as-code/pkg/opscomments"
	"github.com/openshift-pipelines/pipelines-as-code/pkg/params"
	"github.com/openshift-pipelines/pipelines-as-code/pkg/params/info"
	"github.com/openshift-pipelines/pipelines-as-code/pkg/params/triggertype"
	"github.com/openshift-pipelines/pipelines-as-code/pkg/provider"
	"github.com/openshift-pipelines/pipelines-as-code/pkg/secrets"

	gitlab "gitlab.com/gitlab-org/api/client-go"
)

func (v *Provider) ParsePayload(ctx context.Context, run *params.Run, request *http.Request,
	payload string,
) (*info.Event, error) {
	event := request.Header.Get("X-Gitlab-Event")
	if event == "" {
		return nil, fmt.Errorf("failed to find event type in request header")
	}

	payloadB := []byte(payload)
	eventInt, err := gitlab.ParseWebhook(gitlab.EventType(event), payloadB)
	if err != nil {
		return nil, err
	}
	_ = json.Unmarshal(payloadB, &eventInt)

	// Remove the " Hook" suffix so looks better in status, and since we don't
	// really use it anymore we good to do whatever we want with it for
	// cosmetics.
	processedEvent := info.NewEvent()
	processedEvent.EventType = strings.ReplaceAll(event, " Hook", "")
	processedEvent.Event = eventInt
	switch gitEvent := eventInt.(type) {
	case *gitlab.MergeEvent:
		// Organization:  event.GetRepo().GetOwner().GetLogin(),
		processedEvent.Sender = gitEvent.User.Username
		processedEvent.DefaultBranch = gitEvent.Project.DefaultBranch
		processedEvent.URL = gitEvent.Project.WebURL
		processedEvent.SHA = gitEvent.ObjectAttributes.LastCommit.ID
		processedEvent.SHAURL = gitEvent.ObjectAttributes.LastCommit.URL
		processedEvent.SHATitle = gitEvent.ObjectAttributes.LastCommit.Title
		processedEvent.SHAMessage = gitEvent.ObjectAttributes.LastCommit.Message
		processedEvent.HeadBranch = gitEvent.ObjectAttributes.SourceBranch
		processedEvent.BaseBranch = gitEvent.ObjectAttributes.TargetBranch
		processedEvent.HeadURL = gitEvent.ObjectAttributes.Source.WebURL
		processedEvent.BaseURL = gitEvent.ObjectAttributes.Target.WebURL
		processedEvent.PullRequestNumber = int(gitEvent.ObjectAttributes.IID)
		processedEvent.PullRequestTitle = gitEvent.ObjectAttributes.Title
		v.targetProjectID = gitEvent.Project.ID
		v.sourceProjectID = gitEvent.ObjectAttributes.SourceProjectID
		v.userID = gitEvent.User.ID

		v.pathWithNamespace = gitEvent.ObjectAttributes.Target.PathWithNamespace
		processedEvent.Organization, processedEvent.Repository = getOrgRepo(v.pathWithNamespace)
		processedEvent.SourceProjectID = gitEvent.ObjectAttributes.SourceProjectID
		processedEvent.TargetProjectID = gitEvent.Project.ID

		processedEvent.TriggerTarget = triggertype.PullRequest
		processedEvent.EventType = strings.ReplaceAll(event, " Hook", "")

		// This is a label update, like adding or removing a label from a MR.
		if gitEvent.Changes.Labels.Current != nil {
			processedEvent.EventType = triggertype.PullRequestLabeled.String()
		}
		for _, label := range gitEvent.Labels {
			processedEvent.PullRequestLabel = append(processedEvent.PullRequestLabel, label.Title)
		}
		if gitEvent.ObjectAttributes.Action == "close" {
			processedEvent.TriggerTarget = triggertype.PullRequestClosed
		}
	case *gitlab.TagEvent:
		// GitLab sends same event for both Tag creation and deletion i.e. "Tag Push Hook".
		// if gitEvent.After is containing all zeros and gitEvent.CheckoutSHA is empty
		// it is Delete "Tag Push Hook".
		if provider.IsZeroSHA(gitEvent.After) && gitEvent.CheckoutSHA == "" {
			return nil, fmt.Errorf("event Delete %s is not supported", event)
		}

		// sometime in gitlab tag push event contains no commit
		// in this case we're not supposed to process the event.
		if len(gitEvent.Commits) == 0 {
			return nil, fmt.Errorf("no commits attached to this %s event", event)
		}

		lastCommitIdx := len(gitEvent.Commits) - 1
		processedEvent.Sender = gitEvent.UserUsername
		processedEvent.DefaultBranch = gitEvent.Project.DefaultBranch
		processedEvent.URL = gitEvent.Project.WebURL
		processedEvent.SHA = gitEvent.Commits[lastCommitIdx].ID
		processedEvent.SHAURL = gitEvent.Commits[lastCommitIdx].URL
		processedEvent.SHATitle = gitEvent.Commits[lastCommitIdx].Title
		processedEvent.HeadBranch = gitEvent.Ref
		processedEvent.BaseBranch = gitEvent.Ref
		processedEvent.HeadURL = gitEvent.Project.WebURL
		processedEvent.BaseURL = processedEvent.HeadURL
		processedEvent.TriggerTarget = "push"
		v.pathWithNamespace = gitEvent.Project.PathWithNamespace
		processedEvent.Organization, processedEvent.Repository = getOrgRepo(v.pathWithNamespace)
		v.targetProjectID = gitEvent.ProjectID
		v.sourceProjectID = gitEvent.ProjectID
		v.userID = gitEvent.UserID
		processedEvent.SourceProjectID = gitEvent.ProjectID
		processedEvent.TargetProjectID = gitEvent.ProjectID
		processedEvent.EventType = strings.ReplaceAll(event, " Hook", "")
	case *gitlab.PushEvent:
		if len(gitEvent.Commits) == 0 {
			if !isBranchCreationPayload(gitEvent) {
				return nil, fmt.Errorf("no commits attached to this push event")
			}
			// After is the immutable branch tip when GitLab creates a branch without adding commits.
			processedEvent.SHA = gitEvent.After
			processedEvent.CommitMetadataIncomplete = true
			processedEvent.PipelineRunSourceRevision = gitEvent.After
		} else {
			lastCommitIdx := len(gitEvent.Commits) - 1
			processedEvent.SHA = gitEvent.Commits[lastCommitIdx].ID
			processedEvent.SHAURL = gitEvent.Commits[lastCommitIdx].URL
			processedEvent.SHATitle = gitEvent.Commits[lastCommitIdx].Title
		}
		processedEvent.Sender = gitEvent.UserUsername
		processedEvent.DefaultBranch = gitEvent.Project.DefaultBranch
		processedEvent.URL = gitEvent.Project.WebURL
		processedEvent.HeadBranch = gitEvent.Ref
		processedEvent.BaseBranch = gitEvent.Ref
		processedEvent.HeadURL = gitEvent.Project.WebURL
		processedEvent.BaseURL = processedEvent.HeadURL
		processedEvent.TriggerTarget = "push"
		v.pathWithNamespace = gitEvent.Project.PathWithNamespace
		processedEvent.Organization, processedEvent.Repository = getOrgRepo(v.pathWithNamespace)
		v.targetProjectID = gitEvent.ProjectID
		v.sourceProjectID = gitEvent.ProjectID
		v.userID = gitEvent.UserID
		processedEvent.SourceProjectID = gitEvent.ProjectID
		processedEvent.TargetProjectID = gitEvent.ProjectID
		processedEvent.EventType = strings.ToLower(strings.ReplaceAll(event, " Hook", ""))
	case *gitlab.MergeCommentEvent:
		processedEvent.Sender = gitEvent.User.Username
		processedEvent.DefaultBranch = gitEvent.Project.DefaultBranch
		processedEvent.URL = gitEvent.Project.WebURL
		processedEvent.SHA = gitEvent.MergeRequest.LastCommit.ID
		processedEvent.SHAURL = gitEvent.MergeRequest.LastCommit.URL
		processedEvent.SHATitle = gitEvent.MergeRequest.LastCommit.Title
		processedEvent.BaseBranch = gitEvent.MergeRequest.TargetBranch
		processedEvent.HeadBranch = gitEvent.MergeRequest.SourceBranch
		processedEvent.BaseURL = gitEvent.MergeRequest.Target.WebURL
		processedEvent.HeadURL = gitEvent.MergeRequest.Source.WebURL

		opscomments.SetEventTypeAndTargetPR(processedEvent, gitEvent.ObjectAttributes.Note, "/")
		v.pathWithNamespace = gitEvent.Project.PathWithNamespace
		processedEvent.Organization, processedEvent.Repository = getOrgRepo(v.pathWithNamespace)
		processedEvent.TriggerTarget = triggertype.PullRequest

		processedEvent.PullRequestNumber = int(gitEvent.MergeRequest.IID)
		v.targetProjectID = gitEvent.MergeRequest.TargetProjectID
		v.sourceProjectID = gitEvent.MergeRequest.SourceProjectID
		v.userID = gitEvent.User.ID
		processedEvent.SourceProjectID = gitEvent.MergeRequest.SourceProjectID
		processedEvent.TargetProjectID = gitEvent.MergeRequest.TargetProjectID
	case *gitlab.CommitCommentEvent:
		// need run in fetching repository
		v.run = run
		return v.handleCommitCommentEvent(ctx, gitEvent, processedEvent)
	default:
		return nil, fmt.Errorf("event %s is not supported", event)
	}

	v.repoURL = processedEvent.URL
	return processedEvent, nil
}

func isBranchCreationPayload(event *gitlab.PushEvent) bool {
	const branchRefPrefix = "refs/heads/"
	return strings.HasPrefix(event.Ref, branchRefPrefix) &&
		strings.TrimPrefix(event.Ref, branchRefPrefix) != "" &&
		provider.IsZeroSHA(event.Before) &&
		isValidCommitSHA(event.After)
}

func isValidCommitSHA(sha string) bool {
	if len(sha) != 40 || provider.IsZeroSHA(sha) {
		return false
	}
	_, err := hex.DecodeString(sha)
	return err == nil
}

func (v *Provider) initGitLabClient(ctx context.Context, event *info.Event) (*info.Event, error) {
	// This is to ensure the base URL of the client is not reinitialized during tests.
	if v.gitlabClient != nil {
		return event, nil
	}

	// need repo here to get secret info and create gitlab api client
	repo, err := matcher.MatchEventURLRepo(ctx, v.run, event, "")
	if err != nil {
		return event, err
	}

	if repo == nil {
		return event, fmt.Errorf("cannot find a repository match for %s", event.URL)
	}

	// should check global repository for secrets
	secretNS := repo.GetNamespace()
	inheritedGlobalSecret := false
	globalRepo, err := v.run.GetRepository(ctx, v.run.Info.Kube.Namespace, v.run.Info.Controller.GlobalRepository)
	if err == nil && globalRepo != nil {
		if secretNS, inheritedGlobalSecret, err = secrets.ResolveInheritedSecret(repo, globalRepo); err != nil {
			return event, err
		}
		repo.Spec.Merge(globalRepo.Spec)
	}

	kubeInterface, err := kubeinteraction.NewKubernetesInteraction(v.run)
	if err != nil {
		return event, err
	}

	scm := secrets.SecretFromRepository{
		K8int:                 kubeInterface,
		Config:                v.GetConfig(),
		Event:                 event,
		Repo:                  repo,
		WebhookType:           v.pacInfo.WebhookType,
		Logger:                v.Logger,
		Namespace:             secretNS,
		InheritedGlobalSecret: inheritedGlobalSecret,
	}
	if err := scm.Get(ctx); err != nil {
		return event, fmt.Errorf("cannot get secret from repository: %w", err)
	}

	err = v.setClient(ctx, v.run, event, repo, v.eventEmitter, false)
	if err != nil {
		return event, err
	}
	return event, nil
}

func (v *Provider) handleCommitCommentEvent(ctx context.Context, event *gitlab.CommitCommentEvent, processedEvent *info.Event) (*info.Event, error) {
	action := "trigger"
	if event.Repository == nil {
		return nil, fmt.Errorf("error parse_payload: the repository in event payload must not be nil")
	}
	// since comment is made on pushed commit, SourceProjectID and TargetProjectID will be equal.
	v.sourceProjectID = event.ProjectID
	v.targetProjectID = event.ProjectID
	processedEvent.SourceProjectID = v.sourceProjectID
	processedEvent.TargetProjectID = v.targetProjectID
	v.userID = event.User.ID
	v.pathWithNamespace = event.Project.PathWithNamespace
	processedEvent.Organization, processedEvent.Repository = getOrgRepo(v.pathWithNamespace)
	processedEvent.Sender = event.User.Username
	processedEvent.Provider.User = processedEvent.Sender
	processedEvent.URL = event.Project.WebURL
	processedEvent.SHA = event.ObjectAttributes.CommitID
	processedEvent.SHATitle = event.Commit.Title
	processedEvent.HeadURL = processedEvent.URL
	processedEvent.BaseURL = processedEvent.URL
	processedEvent.TriggerTarget = triggertype.Push
	opscomments.SetEventTypeAndTargetPR(processedEvent, event.ObjectAttributes.Note, "/")
	// Set Head and Base branch to default_branch of the repo as this comment is made on
	// a pushed commit.
	defaultBranch := event.Project.DefaultBranch
	processedEvent.HeadBranch, processedEvent.BaseBranch = defaultBranch, defaultBranch
	processedEvent.DefaultBranch = defaultBranch

	var (
		branchName string
		prName     string
		tagName    string
		err        error
	)

	// since we're going to make an API call to ensure that the commit is HEAD of the branch
	// therefore we need to initialize GitLab client here
	processedEvent, err = v.initGitLabClient(ctx, processedEvent)
	if err != nil {
		return processedEvent, err
	}

	// get PipelineRun name from comment if it does contain e.g. `/test pr7`
	if provider.IsTestRetestComment(event.ObjectAttributes.Note) {
		prName, branchName, tagName, err = provider.GetPipelineRunAndBranchOrTagNameFromTestComment(event.ObjectAttributes.Note, "/")
		if err != nil {
			return processedEvent, err
		}
		processedEvent.TargetTestPipelineRun = prName
	}

	if provider.IsCancelComment(event.ObjectAttributes.Note) {
		action = "cancellation"
		prName, branchName, tagName, err = provider.GetPipelineRunAndBranchOrTagNameFromCancelComment(event.ObjectAttributes.Note, "/")
		if err != nil {
			return processedEvent, err
		}
		processedEvent.CancelPipelineRuns = true
		processedEvent.TargetCancelPipelineRun = prName
	}

	if tagName != "" {
		tagPath := fmt.Sprintf("refs/tags/%s", tagName)
		tag, _, err := v.gitlabClient.Tags.GetTag(v.sourceProjectID, tagName)
		if err != nil {
			return processedEvent, fmt.Errorf("error getting tag %s: %w", tagName, err)
		}

		if tag.Commit.ID != processedEvent.SHA {
			return processedEvent, fmt.Errorf("provided SHA %s is not the tagged commit for the tag %s", processedEvent.SHA, tagName)
		}

		processedEvent.HeadBranch = tagPath
		processedEvent.BaseBranch = tagPath
		return processedEvent, nil
	}

	if branchName == "" {
		branchName = processedEvent.HeadBranch
	}

	// check if the commit on which comment is made, is HEAD commit of the branch
	if err := v.isHeadCommitOfBranch(processedEvent, branchName); err != nil {
		if provider.IsCancelComment(event.ObjectAttributes.Note) {
			processedEvent.CancelPipelineRuns = false
		}
		return processedEvent, err
	}

	processedEvent.HeadBranch = branchName
	processedEvent.BaseBranch = branchName
	v.Logger.Infof("gitlab commit_comment: pipelinerun %s has been requested on %s/%s#%s", action, processedEvent.Organization, processedEvent.Repository, processedEvent.SHA)
	return processedEvent, nil
}

// ValidateMergeRequestPayloadExample is a temporary coverage example.
// It is not used to accept or reject incoming webhook events.
func ValidateMergeRequestPayloadExample(event *gitlab.MergeEvent) []string {
	if event == nil {
		return []string{"merge request event is missing"}
	}

	var issues []string

	if event.ObjectKind == "" {
		issues = append(issues, "object kind is missing")
	}
	if event.ObjectKind != "" && event.ObjectKind != "merge_request" {
		issues = append(issues, "object kind is not merge_request")
	}
	if event.EventType == "" {
		issues = append(issues, "event type is missing")
	}
	if event.EventType != "" && event.EventType != "merge_request" {
		issues = append(issues, "event type is not merge_request")
	}
	if event.Project.ID <= 0 {
		issues = append(issues, "project ID must be positive")
	}
	if event.Project.Name == "" {
		issues = append(issues, "project name is missing")
	}
	if event.Project.Name != strings.TrimSpace(event.Project.Name) {
		issues = append(issues, "project name has surrounding whitespace")
	}
	if event.Project.Namespace == "" {
		issues = append(issues, "project namespace is missing")
	}
	if event.Project.PathWithNamespace == "" {
		issues = append(issues, "project path is missing")
	}
	if strings.HasPrefix(event.Project.PathWithNamespace, "/") {
		issues = append(issues, "project path starts with a slash")
	}
	if strings.HasSuffix(event.Project.PathWithNamespace, "/") {
		issues = append(issues, "project path ends with a slash")
	}
	if strings.Contains(event.Project.PathWithNamespace, "//") {
		issues = append(issues, "project path contains an empty segment")
	}
	if event.Project.DefaultBranch == "" {
		issues = append(issues, "default branch is missing")
	}
	if event.Project.DefaultBranch != strings.TrimSpace(event.Project.DefaultBranch) {
		issues = append(issues, "default branch has surrounding whitespace")
	}
	if strings.ContainsAny(event.Project.DefaultBranch, "\r\n") {
		issues = append(issues, "default branch contains a line break")
	}
	if event.Project.WebURL == "" {
		issues = append(issues, "project web URL is missing")
	}
	if event.Project.WebURL != "" && !strings.HasPrefix(event.Project.WebURL, "https://") && !strings.HasPrefix(event.Project.WebURL, "http://") {
		issues = append(issues, "project web URL has an unsupported scheme")
	}
	if strings.ContainsAny(event.Project.WebURL, "\r\n") {
		issues = append(issues, "project web URL contains a line break")
	}
	if event.Project.GitHTTPURL != "" && !strings.HasPrefix(event.Project.GitHTTPURL, "https://") && !strings.HasPrefix(event.Project.GitHTTPURL, "http://") {
		issues = append(issues, "project Git HTTP URL has an unsupported scheme")
	}
	if event.ObjectAttributes.ID <= 0 {
		issues = append(issues, "merge request ID must be positive")
	}
	if event.ObjectAttributes.IID <= 0 {
		issues = append(issues, "merge request IID must be positive")
	}
	if event.ObjectAttributes.SourceProjectID <= 0 {
		issues = append(issues, "source project ID must be positive")
	}
	if event.ObjectAttributes.TargetProjectID <= 0 {
		issues = append(issues, "target project ID must be positive")
	}
	if event.ObjectAttributes.AuthorID <= 0 {
		issues = append(issues, "author ID must be positive")
	}
	if event.ObjectAttributes.SourceBranch == "" {
		issues = append(issues, "source branch is missing")
	}
	if event.ObjectAttributes.TargetBranch == "" {
		issues = append(issues, "target branch is missing")
	}
	if event.ObjectAttributes.SourceBranch != strings.TrimSpace(event.ObjectAttributes.SourceBranch) {
		issues = append(issues, "source branch has surrounding whitespace")
	}
	if event.ObjectAttributes.TargetBranch != strings.TrimSpace(event.ObjectAttributes.TargetBranch) {
		issues = append(issues, "target branch has surrounding whitespace")
	}
	if strings.ContainsAny(event.ObjectAttributes.SourceBranch, "\r\n") {
		issues = append(issues, "source branch contains a line break")
	}
	if strings.ContainsAny(event.ObjectAttributes.TargetBranch, "\r\n") {
		issues = append(issues, "target branch contains a line break")
	}
	if event.ObjectAttributes.SourceBranch == event.ObjectAttributes.TargetBranch && event.ObjectAttributes.SourceProjectID == event.ObjectAttributes.TargetProjectID {
		issues = append(issues, "source and target identify the same branch")
	}
	if event.ObjectAttributes.Title == "" {
		issues = append(issues, "merge request title is missing")
	}
	if event.ObjectAttributes.Title != strings.TrimSpace(event.ObjectAttributes.Title) {
		issues = append(issues, "merge request title has surrounding whitespace")
	}
	if strings.ContainsAny(event.ObjectAttributes.Title, "\r\n") {
		issues = append(issues, "merge request title contains a line break")
	}
	if len(event.ObjectAttributes.Title) > 1024 {
		issues = append(issues, "merge request title is too long")
	}
	if event.ObjectAttributes.State == "" {
		issues = append(issues, "merge request state is missing")
	}
	if event.ObjectAttributes.State != "" && event.ObjectAttributes.State != "opened" && event.ObjectAttributes.State != "closed" && event.ObjectAttributes.State != "merged" {
		issues = append(issues, "merge request state is unexpected")
	}
	if event.ObjectAttributes.Action == "" {
		issues = append(issues, "merge request action is missing")
	}
	if strings.ContainsAny(event.ObjectAttributes.Action, "\r\n") {
		issues = append(issues, "merge request action contains a line break")
	}
	if event.ObjectAttributes.URL == "" {
		issues = append(issues, "merge request URL is missing")
	}
	if event.ObjectAttributes.URL != "" && !strings.HasPrefix(event.ObjectAttributes.URL, "https://") && !strings.HasPrefix(event.ObjectAttributes.URL, "http://") {
		issues = append(issues, "merge request URL has an unsupported scheme")
	}
	if strings.ContainsAny(event.ObjectAttributes.URL, "\r\n") {
		issues = append(issues, "merge request URL contains a line break")
	}
	if event.ObjectAttributes.LastCommit.ID == "" {
		issues = append(issues, "last commit ID is missing")
	}
	if event.ObjectAttributes.LastCommit.ID != "" && !isValidCommitSHA(event.ObjectAttributes.LastCommit.ID) {
		issues = append(issues, "last commit ID is not a valid SHA")
	}
	if event.ObjectAttributes.LastCommit.Title == "" {
		issues = append(issues, "last commit title is missing")
	}
	if strings.ContainsAny(event.ObjectAttributes.LastCommit.Title, "\r\n") {
		issues = append(issues, "last commit title contains a line break")
	}
	if event.ObjectAttributes.LastCommit.URL == "" {
		issues = append(issues, "last commit URL is missing")
	}
	if event.ObjectAttributes.LastCommit.URL != "" && !strings.HasPrefix(event.ObjectAttributes.LastCommit.URL, "https://") && !strings.HasPrefix(event.ObjectAttributes.LastCommit.URL, "http://") {
		issues = append(issues, "last commit URL has an unsupported scheme")
	}
	if event.ObjectAttributes.TimeEstimate < 0 {
		issues = append(issues, "time estimate is negative")
	}
	if event.ObjectAttributes.TotalTimeSpent < 0 {
		issues = append(issues, "total time spent is negative")
	}
	if event.ObjectAttributes.LockVersion < 0 {
		issues = append(issues, "lock version is negative")
	}
	if event.ObjectAttributes.Position < 0 {
		issues = append(issues, "position is negative")
	}
	if event.ObjectAttributes.AssigneeID < 0 {
		issues = append(issues, "assignee ID is negative")
	}
	if event.ObjectAttributes.MilestoneID < 0 {
		issues = append(issues, "milestone ID is negative")
	}
	if event.ObjectAttributes.UpdatedByID < 0 {
		issues = append(issues, "updated by ID is negative")
	}
	if event.ObjectAttributes.LastEditedByID < 0 {
		issues = append(issues, "last edited by ID is negative")
	}
	if event.ObjectAttributes.MergeUserID < 0 {
		issues = append(issues, "merge user ID is negative")
	}
	if event.ObjectAttributes.MergeCommitSHA != "" && !isValidCommitSHA(event.ObjectAttributes.MergeCommitSHA) {
		issues = append(issues, "merge commit SHA is invalid")
	}
	if event.ObjectAttributes.RebaseCommitSHA != "" && !isValidCommitSHA(event.ObjectAttributes.RebaseCommitSHA) {
		issues = append(issues, "rebase commit SHA is invalid")
	}
	if event.ObjectAttributes.InProgressMergeCommitSHA != "" && !isValidCommitSHA(event.ObjectAttributes.InProgressMergeCommitSHA) {
		issues = append(issues, "in-progress merge commit SHA is invalid")
	}
	if event.ObjectAttributes.OldRev != "" && !isValidCommitSHA(event.ObjectAttributes.OldRev) {
		issues = append(issues, "old revision is invalid")
	}
	if event.ObjectAttributes.Source == nil {
		issues = append(issues, "source repository is missing")
	}
	if event.ObjectAttributes.Target == nil {
		issues = append(issues, "target repository is missing")
	}
	if event.Project.ID > 0 && event.ObjectAttributes.TargetProjectID > 0 && event.Project.ID != event.ObjectAttributes.TargetProjectID {
		issues = append(issues, "target project ID does not match the project")
	}

	if event.User == nil {
		issues = append(issues, "user is missing")
	} else {
		if event.User.ID <= 0 {
			issues = append(issues, "user ID must be positive")
		}
		if event.User.Username == "" {
			issues = append(issues, "username is missing")
		}
		if event.User.Username != strings.TrimSpace(event.User.Username) {
			issues = append(issues, "username has surrounding whitespace")
		}
		if strings.ContainsAny(event.User.Username, "\r\n") {
			issues = append(issues, "username contains a line break")
		}
	}

	for index, label := range event.Labels {
		if label == nil {
			issues = append(issues, fmt.Sprintf("label %d is missing", index))
			continue
		}
		if label.ID <= 0 {
			issues = append(issues, fmt.Sprintf("label %d has an invalid ID", index))
		}
		if label.Title == "" {
			issues = append(issues, fmt.Sprintf("label %d has no title", index))
		}
		if strings.ContainsAny(label.Title, "\r\n") {
			issues = append(issues, fmt.Sprintf("label %d has a line break in its title", index))
		}
	}

	return issues
}
