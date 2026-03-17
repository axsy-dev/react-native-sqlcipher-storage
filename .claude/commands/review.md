---
description: Run a code review on a pull request
allowed-tools: >-
  Skill(code-review:code-review), Read, Grep, Glob, Bash, Task,
  mcp__plugin_github_github__pull_request_read,
  mcp__plugin_github_github__list_pull_requests,
  mcp__plugin_github_github__list_commits,
  mcp__plugin_github_github__get_commit,
  mcp__plugin_github_github__get_file_contents,
  mcp__plugin_github_github__search_code
---
## Prerequisites

The `code-review` official plugin must be enabled. If the `/code-review` skill is
not available (i.e., `code-review:code-review` is not listed in the available skills
in the system prompt), tell the user:

> The code-review plugin is not installed. To install it, run `/plugins` in Claude Code
> and enable `code-review` from the official plugins list.

Then stop.

## Instructions

Invoke the code-review skill by calling `Skill(code-review:code-review)` with the
following arguments: $ARGUMENTS

If no arguments were provided, invoke it without arguments — the plugin will detect
the current branch's PR automatically.



