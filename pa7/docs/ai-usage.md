# AI Usage Note — PA7

I used ChatGPT as an assistant while completing PA7. It helped me understand Temporal workflows, implement and troubleshoot the TypeScript activities and checkout saga, interpret test failures, and prepare ADR-006.

With AI assistance, I worked on the six HTTP activities, the four-step checkout workflow, retry and timeout handling, compensation logic, and documentation.

I learned that Temporal stores workflow execution history and can resume a workflow after a worker crashes. I also learned how non-retryable business failures differ from temporary failures, why compensation must happen in reverse order, and why activities must be designed for possible repeated execution.

I used Docker and the provided Vitest suite to check the implementation. All 13 public tests passed, including the automated worker crash-recovery test. I also verified that the TypeScript compilation check completed without errors.

AI was used for guidance, implementation assistance, debugging, and documentation. The final solution was tested against the provided public requirements. Hidden-test results and the final grade are not yet known.