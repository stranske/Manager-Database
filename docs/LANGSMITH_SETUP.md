# LangSmith Setup

Recommended project name: `manager-database`

Recommended tags:
- `filing-summary`
- `holdings-analysis`
- `nl-query`
- `rag-search`

Recommended dashboard panels:
- Latency by chain type: p50, p95, p99
- Error rate: errors divided by total invocations per day
- Token usage: input and output tokens per chain per day
- User feedback: average rating per chain over time
- Evaluation scores: weekly evaluation suite summaries

Suggested alert rules:
- latency p95 greater than 10 seconds -> Slack notification
- error rate greater than 5 percent -> email alert
- evaluation accuracy drop greater than 10 percent week-over-week -> email alert

Environment variables:
- `LANGSMITH_API_KEY`
- `LANGSMITH_PROJECT` (defaults to `manager-database`)
- `LANGSMITH_BASE_URL` if using a non-default deployment

Operational notes:
- Tracing is enabled automatically when `LANGSMITH_API_KEY` is present.
- Weekly quality runs are defined in `etl/evaluation_flow.py`.
- User feedback is stored locally via `/api/chat/feedback` and forwarded to LangSmith when possible.
