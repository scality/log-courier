---
name: review-pr
description: Review a PR on log-courier (Go consumer that reads logs from ClickHouse and writes S3 access log objects)
argument-hint: <pr-number-or-url>
disable-model-invocation: true
allowed-tools: Bash(gh repo view *), Bash(gh pr view *), Bash(gh pr diff *), Bash(gh pr comment *), Bash(gh api *), Bash(git diff *), Bash(git log *), Bash(git show *)
---

# Review GitHub PR

You are an expert code reviewer. Review this PR using the arguments provided.

## Determine PR target

Parse `$ARGUMENTS` to extract the repo and PR number:

- If arguments contain `REPO:` and `PR_NUMBER:` (CI mode), use those values directly.
- If the argument is a GitHub URL (starts with `https://github.com/`), extract `owner/repo` and the PR number from it.
- If the argument is just a number, use the current repo from `gh repo view --json nameWithOwner -q .nameWithOwner`.

## Output mode

- **CI mode** (arguments contain `REPO:` and `PR_NUMBER:`): post inline comments and summary to GitHub.
- **Local mode** (all other cases): output the review as text directly. Do NOT post anything to GitHub.

## Steps

1. **Fetch PR details:**

```bash
gh pr view <number> --repo <owner/repo> --json title,body,headRefOid,author,files
gh pr diff <number> --repo <owner/repo>
```

2. **Read changed files** to understand the full context around each change (not just the diff hunks).

3. **Analyze the changes** against these criteria:

| Area | What to check |
|------|---------------|
| Error wrapping | Use `fmt.Errorf("...: %w", err)`, not `%v`. Check that error chains are preserved for `IsPermanentError()` classification. |
| Context propagation | Pass context through call chains, respect cancellation. Verify correct use of the dual context pattern (`ctx` for lifecycle, `workCtx` for in-flight operations). |
| Goroutine leaks | Ensure goroutines have exit conditions. Check for proper cleanup on context cancellation. |
| Triple composite offset | Changes to offset logic (`insertedAt`, `startTime`, `reqID`) must be consistent across `batchfinder.go`, `logfetch.go`, and `offset.go`. |
| OffsetBuffer invariants | Verify cycle boundary flush is preserved. Check that offsets are never committed before S3 upload succeeds. |
| Distributed table usage | Reads from local tables, writes to federated tables — this is intentional. Do not flag as inconsistency. |
| At-least-once delivery | Verify no code path can skip offset commit after successful upload, or commit before upload completes. |
| ClickHouse query safety | Check for SQL injection, unbounded queries, missing `LIMIT` clauses on large tables. |
| AWS SDK error handling | Permanent errors (`NoSuchBucket`, `InvalidAccessKeyId`, `AccessDenied`) are detected by string matching — verify new error types follow this pattern. |
| Configuration | New config options must be in `configspec.go`, have defaults, and be documented. Check backward compatibility. |
| Graceful shutdown | Verify shutdown paths complete in-flight work using `workCtx` (not `ctx`). |
| Security | No credentials in code, proper IAM scoping, no command injection in config values. |
| Test quality | Tests must use Ginkgo/Gomega, call production code (not test inline logic), and not mock behavior they're supposed to test. |
| Breaking changes | Changes to public APIs, config keys, ClickHouse schema, or S3 object format. |

4. **Deliver your review:**

### If CI mode: post to GitHub

#### Part A: Inline file comments

For each specific issue, post a comment on the exact file and line:

```bash
gh api -X POST -H "Accept: application/vnd.github+json" "repos/<owner/repo>/pulls/<number>/comments" -f body=$'Your comment\n\n— Claude Code' -f path="path/to/file" -F line=<line_number> -f side="RIGHT" -f commit_id="<headRefOid>"
```

**The command must stay on a single bash line.** Use `$'...'` quoting for the `-f body=` value, with `\n` for line breaks. Never use `<br>` — it renders as literal text inside code blocks and suggestion blocks.

Each inline comment must:
- Be short and direct — say what's wrong, why it's wrong, and how to fix it in 1-3 sentences
- No filler, no complex words, no long explanations
- When the fix is a concrete line change (not architectural), include a GitHub suggestion block so the author can apply it in one click:
  ````
  ```suggestion
  corrected-line-here
  ```
  ````
  Only suggest when you can show the exact replacement. For architectural or design issues, just describe the problem.
  Example with a suggestion block:
  ```bash
  gh api ... -f body=$'Missing the shared-guidelines update command.\n\n```suggestion\n/plugin update shared-guidelines@scality-agent-hub\n/plugin update scality-skills@scality-agent-hub\n```\n\n— Claude Code' ...
  ```
- Escape single quotes inside `$'...'` as `\'` (e.g., `don\'t`)
- End with: `— Claude Code`

Use the line number from the **new version** of the file (the line number you'd see after the PR is merged), which corresponds to the `line` parameter in the GitHub API.

#### Part B: Summary comment

```bash
gh pr comment <number> --repo <owner/repo> --body $'LGTM\n\nReview by Claude Code'
```

**The command must stay on a single bash line.** Use `$'...'` quoting with `\n` for line breaks.

Do not describe or summarize the PR. For each issue, state the problem on one line, then list one or more suggestions below it:

```
- <issue>
  - <suggestion>
  - <suggestion>
```

If no issues: just say "LGTM". End with: `Review by Claude Code`

### If local mode: output the review as text

Do NOT post anything to GitHub. Instead, output the review directly as text.

For each issue found, output:

```
**<file_path>:<line_number>** — <what's wrong and how to fix it>
```

When the fix is a concrete line change, include a fenced code block showing the suggested replacement.

At the end, output a summary section listing all issues. If no issues: just say "LGTM".

End with: `Review by Claude Code`

## What NOT to do

- Do not comment on markdown formatting preferences
- Do not suggest refactors unrelated to the PR's purpose
- Do not praise code — only flag problems or stay silent
- If no issues are found, post only a summary saying "LGTM"
- Do not flag style issues already covered by golangci-lint (.golangci.yml)
- Do not flag the intentional offset read/write table mismatch (local reads, federated writes) — this is by design
